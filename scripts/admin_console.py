from pprint import PrettyPrinter
from twisted.spread import pb
from twisted.internet import reactor
from twisted.application.internet import TCPClient
from twisted.application.service import Application


class EchoClient(object):

    def __init__(self):
        self._root = None
        self._pp = PrettyPrinter()

    def call(self, *args):
        dfr = self._root.callRemote(*args)
        dfr.addBoth(self.print_response, args)
        return dfr

    def root_received(self, root):
        self._root = root
        #d = result.callRemote("get_data_point", "connections_count")

        self.call("set_config", "PUSH_SHARES__MINING_SLOTS", True)
        self.call("set_config", "PUSH_SHARES__MINING_SHARES", False)

        #d = result.callRemote('explore', 'for_sessions', ('!', 'sess_stats'))
        #d = result.callRemote('explore', 'for_sessions',
        #                      ('limit', 2), ('!', 'sess_convert'))
        #d = result.callRemote('reconnect', -1, 50)

        #d = result.callRemote('cancel_reconnect')
        #d = result.callRemote('get_reconnect_state')
        reactor.callLater(2, reactor.stop)

    def print_response(self, response, args):
        print "%s\n%s" % (args, self._pp.pformat(response))


e = EchoClient()
client_factory = pb.PBClientFactory()
d = client_factory.getRootObject()
d.addCallback(e.root_received)

application = Application("echo")
client_service = TCPClient("localhost", 3201, client_factory)
client_service.setServiceParent(application)
