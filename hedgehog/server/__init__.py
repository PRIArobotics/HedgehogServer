import zmq
import threading
from hedgehog.protocol import sockets, utils


class HedgehogServer(threading.Thread):
    def __init__(self, endpoint, handler, context=None):
        super(HedgehogServer, self).__init__()
        self.context = context or zmq.Context.instance()
        self.endpoint = endpoint
        self.handler = handler
        self.killer = utils.Killer()
        self._poller = None
        self.socket = None

    @property
    def poller(self):
        return self._poller[0]

    @property
    def sockets(self):
        return self._poller[1]

    def register(self, socket, cb):
        self.poller.register(socket)
        self.sockets[socket] = cb

    def unregister(self, socket):
        self.poller.unregister(socket)
        del self.sockets[socket]

    def close(self):
        self.killer.kill()

    def run(self):
        self._poller = zmq.Poller(), {}

        socket = self.context.socket(zmq.ROUTER)
        socket.bind(self.endpoint)
        self.socket = sockets.RouterWrapper(socket)
        def socket_cb():
            ident, msg = self.socket.recv()
            try:
                handler = getattr(self.handler, msg._command_oneof)
            except AttributeError:
                # TODO handle unknown commands
                print(msg._command_oneof + ': unknown command')
            else:
                handler(self.socket, ident, msg)
        self.register(socket, socket_cb)

        killer = self.killer.connect_receiver()
        def killer_cb():
            killer.recv()
            for socket in list(self.sockets.keys()):
                socket.close()
                self.unregister(socket)
        self.register(killer, killer_cb)

        while len(self._poller[1]) > 0:
            for socket, _ in self.poller.poll():
                self.sockets[socket]()
