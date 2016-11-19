#!/usr/bin/python
import random
import time
import datetime
import math
import sys
import traceback
import click
import re

import collections
Command = collections.namedtuple('Command', 'name args')

from twisted.internet import reactor
from twisted.internet import defer

from poolsrv import logger

logger.init_logging(None, None, 'INFO', False)

from stratum.socket_transport import SocketTransportClientFactory
from stratum.custom_exceptions import RemoteServiceException
from stratum.services import ServiceFactory

from mining.simulator import Simulator

log = logger.get_logger()

SIMULATED_BLOCK_NONCE = "cafebabe"

DEBUG = False
VERBOSE = False


@click.command()
@click.option('-h', '--host', default='127.0.0.1', help='Stratum server host name')
@click.option('-p', '--port', default=3333, help='Stratum server port')
@click.option('-v', '--verbose', default=False)
# @click.option('-s', '--subset', default='1/10', help='Stratum server port')
@click.argument('file', metavar="file", type=click.File('r'), required=True)
def main_cli(host, port, verbose, file):
    global POOL_HOST
    global POOL_PORT
    global VERBOSE
    POOL_HOST = host
    POOL_PORT = port
    VERBOSE = verbose

    lines = file.readlines()
    commands = parse_lines('def', lines)

    runners = []
    for runner, jobs in commands.iteritems():
        r = Runner(runner, jobs)
        d = r.run_all()
        runners.append(d)

    def_runners = defer.DeferredList(runners)
    def_runners.addCallbacks(all_done, all_done)

    # Start the reactor to execute the jobs
    reactor.run()


def all_done(result):
    print "All runners finised, stopping reactor ..."
    reactor.stop()


def parse_lines(tag, lines, commands_bu_runner_name=None):
    commands = commands_bu_runner_name
    if commands is None:
        commands = {}

    # Current runner's name
    runner = None
    # If true, we're in the block comment section
    in_block_comment = False

    for line in lines:

        # Remove line comments
        line = line.split('#', 1)[0].split('//', 1)[0].strip()

        # Skip empty lines
        if not len(line):
            continue

        # Handle block comments
        if line == '/*':
            in_block_comment = True
            continue
        if line == '*/':
            in_block_comment = False
            continue
        if in_block_comment:
            continue

        # Handle runner definition
        if line.startswith('['):
            # Take the runner's name
            runner, rest = line[1:].split(']', 1)
            # Add a tag to runner name
            runner = "%s:%s" % (tag, runner)

            # Are we done with the line?
            line = rest.strip()
            if not len(line):
                continue

        # We always need to know a runner for parsing a command (it needs to be assigned)
        if not runner:
            raise "Missing runner definition"

        # Split command name and optional arguments
        cmd_and_args = line.split(None, 1)
        command = cmd_and_args[0]
        if len(cmd_and_args) == 2:
            args = cmd_and_args[1]
        else:
            args = None

        # If we can see the runner for the first time, let's create a command list for it
        if not runner in commands:
            commands[runner] = []

        # Translate text command and args to actual Runner's command
        # It does all necessary testing
        cmd = Runner.make_command(command, args)

        # Store the command to the runners command list
        commands[runner].append(cmd)

    return commands


class Runner(object):
    START = time.time()
    COUNTER = 0

    def __init__(self, name, commands):
        self._next_cmd_idx = 0
        self._stop_command_flag = False
        self._name = name
        self._commands = commands
        self._con_factory = None

        self._methods = {
            'mining.set_difficulty': self._on_set_difficulty,
            'mining.notify': self._on_notify,
            'client.reconnect': self._on_reconnect
        }
        self._subscription = None
        if VERBOSE:
            self.out("Runner created")

    @staticmethod
    def sleep(secs):
        d = defer.Deferred()
        reactor.callLater(secs, d.callback, None)
        return d

    def _report_and_reset_counter(self, reason):

        length = time.time() - self.START
        share_rate = self._subscription['difficulty'] * (self.COUNTER / length)
        self.out("last %d rate: %.02f / %.01fs -> cca %.01f Ghash/s %.03f sh/s (%s)" % \
                (self.COUNTER, self.COUNTER / length, length,
                 share_rate * (2**32) / (1000*1000*1000.), share_rate, reason))
        self.COUNTER = 0
        self.START = time.time()

    def _prepare_reconnect(self, login):
        """
        Returns if it is necessary to reconnect
        """
        if not self._con_factory:
            return True

        if self._subscription and self._subscription['login'] == login:
            if VERBOSE:
                self.out('miner already authorized')
            return False

        self._finish_job(True)
        return True


    @defer.inlineCallbacks
    def _connect(self, login, passwd):
        if self._con_factory or self._subscription:
            raise Exception('Factory or subscription already present')

        # Try to connect to remote server
        self._con_factory = SocketTransportClientFactory(POOL_HOST, POOL_PORT,
                                                         allow_trusted=True,
                                                         allow_untrusted=False,
                                                         debug=DEBUG,
                                                         signing_key=None,
                                                         signing_id=None,
                                                         event_handler=self)
        self.out("connecting... ")
        try:
            yield self._con_factory.on_connect  # Wait to on_connect event
        except:
            self.out('ERR: connection failed')
            raise

        auth_result = (yield self._con_factory.subscribe('mining.authorize', [login, passwd]))
        self.out("authorized %s %s" % (login, str(auth_result)))

        (_, extranonce1, extranonce2_size) = (yield self._con_factory.subscribe('mining.subscribe', ['minersim-1.0',]))[:3]

        self._subscription = {'login': login,
                              'difficulty': 1,
                              'difficulty_new': 1,
                              'job_id': None,
                              'ntime': time.time(),
                              'extranonce2_size': extranonce2_size,
                              'job_specified': defer.Deferred()}
        self.out("subscribed, waiting for a job")

        yield self._subscription['job_specified']

        self.START = time.time()
        self.COUNTER = 0


    @defer.inlineCallbacks
    def _simulate_hashrate(self, login, hash_rate, deterministic):
        prev_submit = None
        prev_submit_time = time.time()

        while not self._stop_command_flag:
            wait_duration = Simulator.get_next_share_wait_time(hash_rate,
                                                               self._subscription['difficulty'],
                                                               deterministic)

            # Compute how far is next submit from the last one
            submit_time = prev_submit_time + wait_duration
            # How long we should wait?
            wait_duration = submit_time - time.time()
            prev_submit_time = submit_time

            if wait_duration <= 0.0:
                wait_duration = 0

            # Wait for the next share
            yield self.sleep(wait_duration)

            if prev_submit:
                # Wait for a response before next submit is made
                yield prev_submit
                prev_submit = None

            if not self._subscription['job_id']:
                continue

            self.COUNTER += 1

            length = time.time() - self.START
            if length >= 50 or self.COUNTER >= 100:
                self._report_and_reset_counter('regular')

            # Generate a random nonce which is NOT interpreted as a new block
            while True:
                nonce = "%08x" % random.randint(0, 0xffffffff)
                if nonce != SIMULATED_BLOCK_NONCE:
                    break

            prev_submit = self._submit(login, nonce)

    def _submit(self, login, nonce):
        submit = self._con_factory.rpc('mining.submit', [login,
                                                       self._subscription['job_id'],
                                                       "01" * self._subscription['extranonce2_size'],
                                                       "%x" % (self._subscription['ntime'] + 1),
                                                       nonce])
        submit.addErrback(self._submission_err)
        return submit

    def _submission_err(self, failure):
        if failure.type == RemoteServiceException:
            error_msg = failure.value.args[1]
            if error_msg in ('Stale share', ) or \
                    re.match("Job '[a-fA-F0-9]+' not found", error_msg):
                self.out("Submission error ignored: %s" % error_msg)
                return None

        self.out("Submission error")
        return failure

    def _block_accepted(self, d):
        self.out('block accepted')

    def _finish_job(self, disconnect):
        if disconnect:
            self.out('disconnecting')
            if self._con_factory:
                if self._con_factory.client:
                    self._con_factory.client.transport.abortConnection()
                if self._con_factory:
                    self._con_factory.stopTrying()
                self._con_factory = None
            self._subscription = None

    def out(self, arg):
        line = "%s [%s] %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                               self._name, arg)
        sys.stdout.write(line)

    def _handle_event(self, msg_method, msg_params, connection):
        self._methods[msg_method](*msg_params)

        return ServiceFactory.call(msg_method, msg_params, connection_ref=connection)

    def _on_set_difficulty(self, difficulty):
        self._subscription['difficulty_new'] = difficulty
        self._report_and_reset_counter('diff changed')
        if VERBOSE:
            self.out("Received difficulty %d" % difficulty)

    def _on_reconnect(self, *args):
        self.out('received RECONNECT from the server')

    def _on_notify(self, job_id, prev_hash, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs, *args):
        # self.out('CALLED on_notify %s' % str((job_id, prev_hash, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean_jobs, args))
        try:
            job_id.lower()
        except:
            self.out("non string job_id %s received" % str(job_id))

        self._subscription['job_id'] = job_id
        self._subscription['ntime'] = int(ntime, 16)

        if self._subscription['job_specified']:
            self._subscription['job_specified'].callback(True)
            self._subscription['job_specified'] = None

        if clean_jobs and self._subscription['difficulty'] != self._subscription['difficulty_new']:
            self._subscription['difficulty'] = self._subscription['difficulty_new']
            self.COUNTER = 0
            self.START = time.time()
        else:
            # print "Job update", SUBSCRIPTION['job_id'], "clean", clean_jobs
            pass

    def __call__(self, *args, **kwargs):
        """
        It is used as an EventHandler class. Instantiation is by-passed by this method.
        """
        return self

    def change_speed(self, lower, upper, period):
        self.__speed = math.exp(math.log(lower) + (random.random() ** 1.5 * (math.log(upper) - math.log(lower))))

        if period:
            reactor.callLater(period, self.change_speed, lower, upper, period)

        self._report_and_reset_counter('speed change')
        self.out("SPEED changed to %0.02f Gh/s %0.03f sh/s" % (self.__speed, self.__speed * (1000*1000*1000.0)/(2**32)))

    def _cmd_loop(self, cmd_index=0):
        self._next_cmd_idx = cmd_index

    def _cmd_disconnect(self):
        self._finish_job(True)

    def _cmd_sim(self, login, passwd, hashrate, duration):
        return self._simulate_cmd_common(login, passwd, hashrate, duration, False)

    def _cmd_simd(self, login, passwd, hashrate, duration):
        return self._simulate_cmd_common(login, passwd, hashrate, duration, True)

    @defer.inlineCallbacks
    def _cmd_block(self, login, passwd):
        if self._prepare_reconnect(login):
            yield self._connect(login, passwd)

        self.out("submitting BLOCK")
        submit = self._submit(login, SIMULATED_BLOCK_NONCE)
        submit.addCallback(self._block_accepted)
        yield submit

    @defer.inlineCallbacks
    def _simulate_cmd_common(self, login, passwd, hashrate, duration, deterministic):
        if duration > 0:
            self._plan_stop_command(duration)

        if self._prepare_reconnect(login):
            yield self._connect(login, passwd)
        yield self._simulate_hashrate(login, hashrate, deterministic)

    @staticmethod
    def _get_interval_integer(interval):
        vals = interval.split('-', 1)
        if len(vals) > 1:
            return random.randint(int(vals[0]), int(vals[1]))
        return int(vals[0])

    @defer.inlineCallbacks
    def _cmd_wait(self, seconds):
        seconds = self._get_interval_integer(seconds)
        yield self.sleep(seconds)

    @defer.inlineCallbacks
    def _cmd_sync(self, seconds):
        to_wait = seconds - (time.time() % seconds)
        yield self.sleep(to_wait)

    def _stop_command(self):
        self._stop_command_flag = True

    def _plan_stop_command(self, delay):
        reactor.callLater(delay, self._stop_command)

    @defer.inlineCallbacks
    def run_all(self):
        self._next_cmd_idx = 0
        while self._next_cmd_idx < len(self._commands):
            cmd = self._commands[self._next_cmd_idx]
            self._next_cmd_idx += 1
            try:
                self._stop_command_flag = False
                yield self._run_command(cmd)
            except:
                self.out(traceback.format_exc())
                self._finish_job(True)

        self.out("Out of jobs")
        defer.returnValue(None)

    @classmethod
    def make_command(cls, cmd_name, args):
        if cmd_name == 'loop':
            cmd_idx = (int(args),) if args else ()
            return Command('loop', cmd_idx)

        if cmd_name in ('sim', 'simd'):
            login, hash_rate, duration = args.split(':')
            login, passwd = login.split("=", 1)
            hash_rate = float(hash_rate)
            if len(duration.strip()):
                duration = float(duration)
            else:
                duration = 0
            return Command(cmd_name, (login, passwd, hash_rate, duration))

        if cmd_name == 'block':
            login, passwd = args.split("=", 1)
            return Command('block', (login, passwd))

        if cmd_name == 'wait':
            if not re.match('[0-9]+(-[0-9]+)?', args):
                raise Exception("Invalid format for wait time: %s" % str(args))
            return Command('wait', (args,))

        if cmd_name == 'sync':
            if not re.match('[0-9]+', args):
                raise Exception("Invalid format for sync: %s" % str(args))
            return Command('wait', (int(args),))

        if cmd_name == 'disconnect':
            if not args is None:
                raise Exception("Disconnect doesn't take any parameters: %s" % str(args))
            return Command('disconnect', ())

        raise Exception("Unknown command %s" % str(cmd_name))

    @defer.inlineCallbacks
    def _run_command(self, cmd):
        func = self.__getattribute__('_cmd_%s' % cmd.name)
        if VERBOSE:
            self.out('executing: %s %s' % (cmd.name, cmd.args))
        yield func(*cmd.args)


if __name__ == '__main__':
    random.seed()
    main_cli()
