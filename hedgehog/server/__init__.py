import zmq
import threading
from hedgehog.utils import zmq as zmq_utils
from hedgehog.protocol import messages, sockets
from hedgehog.protocol.errors import HedgehogCommandError, UnsupportedCommandError

from . import handlers
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.serial import SerialHardwareAdapter


class HedgehogServer:
    def __init__(self, endpoint, handlers, ctx=None):
        if ctx is None:
            ctx = zmq.Context.instance()
        socket = ctx.socket(zmq.ROUTER)
        socket.bind(endpoint)
        self.socket = sockets.DealerRouterWrapper(socket)

        self.pipe, pipe = zmq_utils.pipe(ctx)
        self.poller = zmq_utils.Poller()

        def socket_cb():
            ident, msgs_raw = self.socket.recv_multipart_raw()

            def handle(msg_raw):
                try:
                    msg = messages.parse(msg_raw)
                    try:
                        handler = handlers[msg._command_oneof]
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
            pipe.recv()
            for socket in list(self.poller.sockets):
                socket.close()
                self.unregister(socket)
        self.register(pipe, killer_cb)

        threading.Thread(target=self.run).start()

    def register(self, socket, cb):
        self.poller.register(socket, zmq.POLLIN, cb)

    def unregister(self, socket):
        self.poller.unregister(socket)

    def close(self):
        self.pipe.send(b'')

    def run(self):
        while len(self.poller.sockets) > 0:
            for _, _, cb in self.poller.poll():
                cb()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def handler():
    return handlers.to_dict(HardwareHandler(SerialHardwareAdapter()), ProcessHandler())


def main():
    ctx = zmq.Context.instance()

    server = HedgehogServer('tcp://*:5555', handler(), ctx=ctx)


if __name__ == '__main__':
    main()
