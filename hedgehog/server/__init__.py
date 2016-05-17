import zmq
import threading
from hedgehog.utils import zmq as zmq_utils
from hedgehog.protocol import messages, sockets
from hedgehog.protocol.errors import HedgehogCommandError, UnsupportedCommandError

from . import handlers
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.serial import SerialHardwareAdapter


class HedgehogServer(threading.Thread):
    def __init__(self, endpoint, handlers, ctx=None):
        super().__init__()

        if ctx is None:
            ctx = zmq.Context.instance()
        socket = ctx.socket(zmq.ROUTER)
        socket.bind(endpoint)
        self.socket = sockets.DealerRouterWrapper(socket)

        self.handlers = handlers
        self.pipe = zmq_utils.pipe(ctx)
        self.poller = zmq.Poller()
        self.sockets = {}

    def register(self, socket, cb):
        self.poller.register(socket, zmq.POLLIN)
        self.sockets[socket] = cb

    def unregister(self, socket):
        self.poller.unregister(socket)
        del self.sockets[socket]

    def close(self):
        self.pipe[0].send(b'')

    def run(self):
        def socket_cb():
            ident, msgs_raw = self.socket.recv_multipart_raw()
            def handle(msg_raw):
                try:
                    msg = messages.parse(msg_raw)
                    try:
                        handler = self.handlers[msg._command_oneof]
                    except KeyError as err:
                        raise UnsupportedCommandError(msg._command_oneof)
                    else:
                        return handler(self, ident, msg)
                except HedgehogCommandError as err:
                    return err.to_message()

            msgs = [handle(msg) for msg in msgs_raw]
            self.socket.send_multipart(ident, msgs)
        self.register(self.socket.socket, socket_cb)

        def killer_cb():
            self.pipe[1].recv()
            for socket in list(self.sockets.keys()):
                socket.close()
                self.unregister(socket)
        self.register(self.pipe[1], killer_cb)

        while len(self.sockets) > 0:
            for socket, _ in self.poller.poll():
                self.sockets[socket]()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def handler():
    return handlers.to_dict(HardwareHandler(SerialHardwareAdapter()), ProcessHandler())


def main():
    ctx = zmq.Context.instance()

    server = HedgehogServer('tcp://*:5555', handler(), ctx=ctx)
    server.start()


if __name__ == '__main__':
    main()
