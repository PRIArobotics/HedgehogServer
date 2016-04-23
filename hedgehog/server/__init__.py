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

    def close(self):
        self.killer.kill()

    def run(self):
        socket = self.context.socket(zmq.ROUTER)
        socket.bind(self.endpoint)
        socket = sockets.RouterWrapper(socket)
        killer = self.killer.connect_receiver()

        while True:
            pollin, _, _ = zmq.select([socket.socket, killer], [], [])
            if pollin:
                if socket.socket in pollin:
                    ident, msg = socket.recv()
                    try:
                        handler = getattr(self.handler, msg._command_oneof)
                    except AttributeError:
                        # TODO handle unknown commands
                        print(msg._command_oneof + ': unknown command')
                        pass
                    else:
                        handler(socket, ident, msg)
                if killer in pollin:
                    break

        socket.close()
        killer.close()
