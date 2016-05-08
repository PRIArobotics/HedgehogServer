import zmq
import threading
from hedgehog.protocol import sockets, utils


class HedgehogServer(threading.Thread):
    def __init__(self, endpoint, handlers, context=None):
        super().__init__()
        self.context = context or zmq.Context.instance()
        self.endpoint = endpoint
        self.handlers = handlers
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
        self.poller.register(socket, zmq.POLLIN)
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
        self.socket = sockets.DealerRouterWrapper(socket)

        def socket_cb():
            ident, msgs = self.socket.recv_multipart()
            for msg in msgs:
                try:
                    handler = self.handlers[msg._command_oneof]
                except KeyError:
                    # TODO handle unknown commands
                    print(msg._command_oneof + ': unknown command')
                else:
                    handler(self, ident, msg)
        self.register(socket, socket_cb)

        killer = self.killer.connect_receiver()

        def killer_cb():
            killer.recv()
            for socket in list(self.sockets.keys()):
                socket.close()
                self.unregister(socket)
        self.register(killer, killer_cb)

        while len(self.sockets) > 0:
            for socket, _ in self.poller.poll():
                self.sockets[socket]()
