# Run me with "twistd -ny launcher_demo.tac -l -"

# Run listening when mining service is ready
from twisted.internet import defer
on_startup = defer.Deferred()

# Initialize configurations
import conf.config
from poolsrv import config
config.init_config(conf.config)

#Initialize logging facility
import poolsrv.logger
poolsrv.logger.init_logging(config.LOGFILE,
                            config.LOGDIR,
                            config.LOGLEVEL,
                            config.DEBUG)

# Initialize twisted epoll reactor
import poolsrv.epoll

# Bootstrap Stratum framework
import stratum
application = stratum.setup(on_startup)

# Load mining service into stratum framework
import mining
from iface import worker_manager, share_manager, share_limiter


from mining.interfaces import Interfaces
from mining.interfaces import TimestamperInterface
from mining.interfaces import DefaultIdsProvider
from mining.difficulty import DifficultySubscription
from iface.reporting import StratumReporter
from iface.share_sink import ShareSink
from admin import Admin

Interfaces.set_timestamper(TimestamperInterface())
Interfaces.set_worker_manager(worker_manager.WorkerManager())
Interfaces.set_share_manager(share_manager.ShareManager())
Interfaces.set_share_limiter(share_limiter.ShareLimiter(DifficultySubscription.emit_single_before_mining_notify,
                                                        on_startup))
Interfaces.set_ids_provider(DefaultIdsProvider())
Interfaces.set_reporter(StratumReporter(config.REPORTER_HOST, config.REPORTER_PORT,
                                        max_queue_len=config.REPORTER_QUEUE_LENGTH,
                                        max_active_queue_len=config.REPORTER_ACTIVE_QUEUE_LENGTH))
Interfaces.set_share_sink(ShareSink(config.SHARE_SINK_HOST, config.SHARE_SINK_PORT))
Interfaces.set_admin(Admin())


from twisted.spread import pb
from twisted.application.internet import TCPServer

TCPServer(config.ADMIN_PORT, pb.PBServerFactory(Interfaces.admin)).setServiceParent(application)


# It will fire on_setup callback
mining.setup(on_startup)

from admin.manhole import *
from admin.explore import *
import poolsrv.manhole
from twisted.conch.manhole import ColoredManhole

# Runs python console in the stratum accessible thru ssh or telnet connection
namespace = globals()
poolsrv.manhole.make_service({'protocolFactory': ColoredManhole,
                              'protocolArgs': (namespace,),
                              'telnet': config.MANHOLE_TELNET_PORT,
                              #'ssh': config.MANHOLE_SSH_PORT
                              }).setServiceParent(application)


# If simulation file is defined then run a simulator with the file as input
if config.SIMULATION_FILE is not None:
    from mining.simulator import Simulator
    sim = Simulator(config.SIMULATION_FILE, config.SIMULATION_FILTER)
    on_startup.addCallback(lambda event: sim.start_simulation())
