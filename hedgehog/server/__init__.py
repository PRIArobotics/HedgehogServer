import sys
import zmq
import threading
from hedgehog.utils import zmq as zmq_utils
from hedgehog.utils.discovery.node import Node
from hedgehog.protocol import messages, sockets
from hedgehog.protocol.errors import HedgehogCommandError, UnsupportedCommandError

from . import handlers
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.serial import SerialHardwareAdapter
from hedgehog.server.hardware.logging import LoggingHardwareAdapter


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
                        handler = handlers[msg.meta.discriminator]
                    except KeyError as err:
                        raise UnsupportedCommandError(msg.meta.discriminator)
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

        def poll():
            while len(self.poller.sockets) > 0:
                for _, _, cb in self.poller.poll():
                    cb()

        threading.Thread(target=poll).start()

    def register(self, socket, cb):
        self.poller.register(socket, zmq.POLLIN, cb)

    def unregister(self, socket):
        self.poller.unregister(socket)

    def close(self):
        self.pipe.send(b'')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def handler():
    hardware = SerialHardwareAdapter()
    # hardware = LoggingHardwareAdapter(hardware)
    return handlers.to_dict(HardwareHandler(hardware), ProcessHandler())


def main():
    args = sys.argv[1:]
    port = 0 if len(args) == 0 else args[0]

    ctx = zmq.Context.instance()
    service = 'hedgehog_server'

    node = Node("Hedgehog Server", ctx)
    node.start()
    node.join(service)

    server = HedgehogServer('tcp://*:{}'.format(port), handler(), ctx=ctx)
    node.add_service(service, server.socket.socket)

    print("{} started on {}".format(node.name(), server.socket.socket.last_endpoint.decode('utf-8')))


if __name__ == '__main__':
    main()
