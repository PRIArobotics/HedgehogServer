import logging
import zmq
from pyre.zactor import ZActor
from hedgehog.utils import zmq as zmq_utils
from hedgehog.utils.discovery.node import Node
from hedgehog.protocol import messages, sockets
from hedgehog.protocol.errors import HedgehogCommandError, UnsupportedCommandError

from . import handlers
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.serial import SerialHardwareAdapter
from hedgehog.server.hardware.logging import LoggingHardwareAdapter

logger = logging.getLogger(__name__)


class HedgehogServerActor(object):
    def __init__(self, ctx, pipe, queue, endpoint, handlers):
        if ctx is None:
            ctx = zmq.Context.instance()

        socket = ctx.socket(zmq.ROUTER)
        socket.bind(endpoint)
        self.socket = sockets.DealerRouterWrapper(socket)
        self.handlers = handlers

        self.pipe = pipe
        self.queue = queue

        self.poller = zmq_utils.Poller()
        self.register(self.socket.socket, self.recv_socket)
        self.register(self.pipe, self.recv_api)

        self.pipe.signal()

        while len(self.poller.sockets) > 0:
            for _, _, recv in self.poller.poll():
                recv()

    def recv_socket(self):
        ident, msgs_raw = self.socket.recv_multipart_raw()

        def handle(msg_raw):
            try:
                msg = messages.parse(msg_raw)
                try:
                    handler = self.handlers[msg.meta.discriminator]
                except KeyError as err:
                    raise UnsupportedCommandError(msg.meta.discriminator)
                else:
                    return handler(self, ident, msg)
            except HedgehogCommandError as err:
                return err.to_message()

        msgs = [handle(msg) for msg in msgs_raw]
        self.socket.send_multipart(ident, msgs)

    def recv_api(self):
        command = self.pipe.recv_unicode()
        if command == "SOCK":
            self.queue.append(self.socket.socket)
            self.pipe.signal()
        elif command == "REG":
            socket, cb = self.queue.pop(0)
            self.pipe.signal()
            self.poller.register(socket, zmq.POLLIN, cb)
        elif command == "UNREG":
            socket = self.queue.pop(0)
            self.pipe.signal()
            self.poller.unregister(socket)
        elif command == "$TERM":
            for socket in list(self.poller.sockets):
                socket.close()
                self.unregister(socket)
        else:
            logger.warning("Unkown Node API command: {0}".format(command))

    def register(self, socket, cb):
        self.poller.register(socket, zmq.POLLIN, cb)

    def unregister(self, socket):
        self.poller.unregister(socket)


class HedgehogServer(object):
    def __init__(self, endpoint, handlers, ctx=None):
        if ctx is None:
            ctx = zmq.Context.instance()

        self._socket = None
        self.queue = []
        self.actor = ZActor(ctx, HedgehogServerActor, self.queue, endpoint, handlers)

    @property
    def socket(self):
        if not self._socket:
            self.actor.send_unicode("SOCK")
            self.actor.resolve().wait()
            self._socket = self.queue.pop(0)
        return self._socket

    def register(self, socket, cb):
        self.queue.append((socket, cb))
        self.actor.send_unicode("REG")
        self.actor.resolve().wait()

    def unregister(self, socket):
        self.queue.append(socket)
        self.actor.send_unicode("UNREG")
        self.actor.resolve().wait()

    def close(self):
        self.actor.destroy()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def start(name, hardware, port=0):
    ctx = zmq.Context.instance()
    service = 'hedgehog_server'

    # TODO retry on missing network
    node = Node(name, ctx)
    node.start()
    node.join(service)

    handler = handlers.to_dict(
        HardwareHandler(LoggingHardwareAdapter(hardware())),
        ProcessHandler()
    )

    server = HedgehogServer('tcp://*:{}'.format(port), handler, ctx=ctx)
    node.add_service(service, server.socket)

    logger.info("{} started on {}".format(node.name(), server.socket.last_endpoint.decode('utf-8')))
