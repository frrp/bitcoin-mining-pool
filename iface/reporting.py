from twisted.internet import reactor
import collections

from poolsrv.reporting import JsonReporter

from stratum.connection_registry import ConnectionRegistry
from poolsrv import config

from poolsrv.posix_time import posix_time
from poolsrv.identity import RUNTIME

import poolsrv.logger
log = poolsrv.logger.get_logger()


# Sample events
E_WORKER_AUTHORIZED = []

SHARE_RATE_TO_MHASH_PS = float(2 ** 32) / (1000 * 1000)
SHARE_RATE_TO_GHASH_PS = float(2 ** 32) / (1000 * 1000 * 1000)

SIMPLE_EVENTS = ['SUBS', # new_subscription (session event)
                 'AUTH', # new_authorization (worker stats event)
                 'DIFC', # difficulty changed (session event)
                 'DISC', # session_disconnection (session event)
                 'PING', # session_disconnection - immediate, no authorization (session event)
                 'SYNC'] # sync. process finished  (stratum)

_OTHER_EVENTS = ['INIT', # connection initiated (stratum)
                 'WSCH'] # worker statistics changed (worker stats event)


class StratumReporter(JsonReporter):

    def __init__(self, host, port,
                 max_queue_len = 16000,
                 max_active_queue_len = 250,
                 retry_after_s = 5,
                 start_suspended = False):

        JsonReporter.__init__(self, host, port,
                              max_queue_len=max_queue_len,
                              max_active_queue_len=max_active_queue_len,
                              retry_after_s=retry_after_s,
                              start_suspended=start_suspended)

        self._simple_events = {ev: [] for ev in SIMPLE_EVENTS}
        self._sync_token = None
        self._dropped_from_last_sync = False

        self._ws_current_queue = []
        self._ws_current_noticed = set()

        self._ws_noticed_all = set()
        self._ws_delayed_noticed = collections.deque()
        self._ws_delayed_queues = collections.deque()
        for _ in range(config.REPORTER_WORKER_STATS_PARTS - 1): #@UndefinedVariable
            self._ws_delayed_noticed.append(set())
            self._ws_delayed_queues.append([])

        reactor.callLater(1, self._report_worker_stats) #@UndefinedVariable
        reactor.callLater(1, self._report_all_simple_events) #@UndefinedVariable


    def on_drop(self, obj, reason):
        self._dropped_from_last_sync = True
        log.error("dropped object %s, reason %s" % (str(obj), str(reason)))


    def _report_worker_stats(self):
        try:
            # Take the oldest session queue and put
            expired_queue = self._ws_delayed_queues.popleft()
            expired_noticed = self._ws_delayed_noticed.popleft()

            # Take a timestamp for the reporting
            timestamp = posix_time()

            # Remove the just reporting updates from noticed set
            self._ws_noticed_all.difference_update(expired_noticed)
            # Make events from expired queued session
            get_event = self._work_update_event
            events = [get_event(ws, timestamp) for ws in expired_queue]

            # Add the youngest facts into the delay queues
            self._ws_delayed_queues.append(self._ws_current_queue)
            self._ws_delayed_noticed.append(self._ws_current_noticed)
            self._ws_current_noticed = set()
            self._ws_current_queue = []

            if len(events) > 0:
                # Send the report message with all events as a payload
                msg = ['WSCH', int(timestamp * 1000), events] #@UndefinedVariable
                self._queue.deliver(msg)

        finally:
            reactor.callLater(float(config.REPORTER_WORKER_STATS_DELAY) #@UndefinedVariable \
                              / config.REPORTER_WORKER_STATS_PARTS, #@UndefinedVariable
                              self._report_worker_stats)


    def _received_message(self, msg):
        ''' Messages received from the server are routed here.'''
        cmd = msg[0]
        if cmd == 'FULL_SYNC':
            # Start full synchronization process if not already synchronizing
            start_new = self._sync_token is None
            self._sync_token = msg[1]
            if start_new:
                self._sync_sessions()
        elif cmd == 'FLUSH_SYNC':
            # Do fast/flush synchronization process if not already synchronizing
            start_new = self._sync_token is None
            self._sync_token = msg[1]
            if start_new:
                self._flush_and_send_sync()
        return True


    def _flush_and_send_sync(self):
        # Flush all the simple events to the delivery queue
        self._report_all_simple_events(False)
        # Retry to deliver as much as possible before 'SYNC' will be added to the queue
        self._queue.retry()
        # Send the actual sync event
        self._queue.deliver(['SYNC', self._sync_token, self._dropped_from_last_sync])
        # The sync. process has finished
        self._sync_token = None


    def _connection_made(self):
        # Send synchronization token and other detail in the INIT message, directly to the server
        # without using any queue
        self._queue.process_directly(['INIT', config.STRATUM_UNIQUE_ID, #@UndefinedVariable
                                      int(RUNTIME.created_at * 1000),
                                      RUNTIME.uuid, self._dropped_from_last_sync])


    def _report_all_simple_events(self, replan=True):
        try:
            for ev in SIMPLE_EVENTS:
                self._report_simple_event(ev)
        finally:
            if replan:
                reactor.callLater(config.REPORTER_SIMPLE_EVENTS_PERIOD, #@UndefinedVariable
                                  self._report_all_simple_events)


    def _add_simple_event(self, ev_name, event):
        events = self._simple_events[ev_name]
        events.append(event)
        if len(events) >= 100:
            self._report_simple_event(ev_name)


    def _report_simple_event(self, ev_name):
        events = self._simple_events[ev_name]
        if len(events) > 0:
            self._simple_events[ev_name] = []
            self._queue.deliver([ev_name, int(posix_time() * 1000), events]) #@UndefinedVariable


    def worker_stats_changed(self, worker_stats):
        if not config.REPORTER__WORKER_STATS_CHANGED: #@UndefinedVariable
            return

        ws_id = worker_stats['worker_session_id']
        # If we haven't seen the worker's session stats yet in the last time period
        if ws_id not in self._ws_noticed_all:
            # Add it to the current run
            self._ws_current_queue.append(worker_stats)
            self._ws_current_noticed.add(ws_id)
            self._ws_noticed_all.add(ws_id)


    def _work_update_event(self, worker_stats, timestamp):
        last_share = worker_stats['last_valid_share']
        return (worker_stats['session_id'],
                worker_stats['worker_session_id'],
                worker_stats['valid_submits'],
                worker_stats['valid_shares'],
                # Use relative timestamp (reversed, to be a positive number)
                int(timestamp - last_share) if last_share else None,
                worker_stats['old_submits'],
                worker_stats['stale_submits'],
                worker_stats['invalid_submits'],
                int(worker_stats['wsma_rate_short'] * SHARE_RATE_TO_MHASH_PS),
                int(worker_stats['wsma_rate_long'] * SHARE_RATE_TO_MHASH_PS))


    def new_authorization(self, session, worker_stats):
        if not config.REPORTER__NEW_AUTHORIZATION: #@UndefinedVariable
            return

        event = (session['session_id'],
                 worker_stats['worker_session_id'],
                 worker_stats['worker_id'],
                 worker_stats['worker_name'],
                 int(worker_stats['authorized_at']))

        self._add_simple_event('AUTH', event)


    def new_subscription(self, session):
        if not config.REPORTER__NEW_SUBSCRIPTION: #@UndefinedVariable
            return

        event = (session['session_id'],
                 session['client_ip'],
                 int(session['connected_at']),
                 int(session['subscribed_at']),
                 session['SL_difficulty'],
                 session['unauthorized_submits'])

        self._add_simple_event('SUBS', event)


    def session_disconnected(self, session):
        if not config.REPORTER__SESSION_DISCONNECTED:
            return

        client_ip = session['client_ip']
        # We don't want to report disconnections of haproxy's probing connections.
        if (client_ip in config.REPORTER_IGNORED_LOCAL_IPS
                and session['subscribed_at'] is None):
            # Do not report any event
            return

        workers_summary = [self._worker_disconnected(ws) \
                           for ws in session['SM_worker_stats'].itervalues()]

        # No authorized workers and immediately closed connection (<= 1 second)
        if len(workers_summary) == 0 and \
                (session['disconnected_at'] - session['connected_at']) <= 1:
            # Report only a 'ping' event
            event = (client_ip, session['connected_at'])
            self._add_simple_event('PING', event)

        else:
            # Report full 'DISC' event
            event = (session['session_id'],
                     client_ip,
                     int(session['connected_at']),
                     int(session['subscribed_at']) if session['subscribed_at'] else None,
                     int(session['disconnected_at']),
                     session['SL_difficulty'],
                     session['unauthorized_submits'],
                     workers_summary)
            self._add_simple_event('DISC', event)


    def _worker_disconnected(self, worker_stats):
        last_share = worker_stats['last_valid_share']
        event = [worker_stats['worker_session_id'],
                 worker_stats['worker_id'],
                 worker_stats['worker_name'],
                 int(worker_stats['authorized_at']),
                 worker_stats['valid_submits'],
                 worker_stats['valid_shares'],
                 int(last_share) if last_share else None,
                 worker_stats['old_submits'],
                 worker_stats['stale_submits'],
                 worker_stats['invalid_submits'],
                 int(worker_stats['wsma_rate_short'] * SHARE_RATE_TO_MHASH_PS),
                 int(worker_stats['wsma_rate_long'] * SHARE_RATE_TO_MHASH_PS)]

        return event


    def difficulty_changed(self, session, synchronizing=False):
        if not config.REPORTER__DIFFICULTY_CHANGED: #@UndefinedVariable
            return

        event = [session['session_id'],
                 # We send the submission rate only when it is life event, not in case of
                 # session synchronization
                 None if synchronizing else session['SL_submission_rate'],
                 session['SL_difficulty'],
                 int(session['SL_difficulty_set_at'])]

        self._add_simple_event('DIFC', event)


    def _plan_sync_process(self):
        reactor.callLater(config.REPORTER_SYNC_SESSIONS_BATCH_DELAY_FUNC(), #@UndefinedVariable
                          self._do_sync_sessions)


    def _do_sync_sessions(self):
        start = posix_time() #@UndefinedVariable

        # There are some connections
        if len(self._to_sync_connections) > 0:
            conns = self._to_sync_connections.pop()

            for connection in conns:
                session = connection.get_session()
                # Report all events
                self.new_subscription(session)
                self.difficulty_changed(session)
                for worker_stats in session['SM_worker_stats'].values():
                    self.new_authorization(session, worker_stats)
                    self.worker_stats_changed(worker_stats)

        if len(self._to_sync_connections) > 0:
            self._plan_sync_process()
        else:
            # Flush all messages into delivery queue and send SYNC message
            self._flush_and_send_sync()
            if self._dropped_from_last_sync:
                log.error("Dropped objects from sync. start")

        log.info("Sync. batch took %.3fs" % (posix_time() - start,)) #@UndefinedVariable


    def _sync_sessions(self):
        start = posix_time()

        self._dropped_from_last_sync = False

        all_conns = collections.deque()
        curr_list = []
        count = 0
        for connection_ref in ConnectionRegistry.iterate():
            try:
                curr_list.append(connection_ref())
                count += 1
            except:
                pass
            if count >= config.REPORTER_SYNC_SESSIONS_BATCH_SIZE: #@UndefinedVariable
                all_conns.append(curr_list)
                curr_list = []

        if len(curr_list) > 0:
            all_conns.append(curr_list)
        self._to_sync_connections = all_conns

        self._plan_sync_process()

        log.info("Sync. start took %.3fs" % (posix_time() - start,)) #@UndefinedVariable

