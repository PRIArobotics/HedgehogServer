import logging
import sys
import zmq
from hedgehog.utils.zmq import Active
from hedgehog.utils.zmq.actor import Actor, CommandRegistry
from hedgehog.utils.zmq.poller import Poller
from hedgehog.protocol import ServerSide
from hedgehog.protocol.sockets import DealerRouterSocket
from hedgehog.protocol.errors import HedgehogCommandError, UnsupportedCommandError

logger = logging.getLogger(__name__)


class HedgehogServerActor(object):
    def __init__(self, ctx, cmd_pipe, evt_pipe, endpoint, handlers):
        self.ctx = ctx
        self.cmd_pipe = cmd_pipe
        self.evt_pipe = evt_pipe

        self.socket = DealerRouterSocket(ctx, zmq.ROUTER, side=ServerSide)
        self.socket.bind(endpoint)
        self.handlers = handlers

        self.poller = Poller()
        self.register_cmd_pipe()
        self.register_socket()

        self.run()

    def register_cmd_pipe(self):
        registry = CommandRegistry()
        self.register(self.cmd_pipe, lambda: registry.handle(self.cmd_pipe.recv_multipart()))

        @registry.command(b'SOCK')
        def handle_sock():
            self.cmd_pipe.push(self.socket)
            self.cmd_pipe.signal()

        @registry.command(b'REG')
        def handle_reg():
            socket, cb = self.cmd_pipe.pop()
            self.poller.register(socket, zmq.POLLIN, cb)

        @registry.command(b'UNREG')
        def handle_unreg():
            socket = self.cmd_pipe.pop()
            self.poller.unregister(socket)

        @registry.command(b'$TERM')
        def handle_term():
            self.terminate()

    def register_socket(self):
        def handle(ident, msg_raw):
            try:
                msg = ServerSide.parse(msg_raw)
                logger.debug("Receive command: %s", msg)
                try:
                    handler = self.handlers[msg.meta.discriminator]
                except KeyError:
                    raise UnsupportedCommandError(msg.__class__.msg_name())
                else:
                    result = handler(self, ident, msg)
            except HedgehogCommandError as err:
                result = err.to_message()
            logger.debug("Send reply:      %s", result)
            return ServerSide.serialize(result)

        def recv_socket():
            ident, msgs_raw = self.socket.recv_msgs_raw()
            self.socket.send_msgs_raw(ident, [handle(ident, msg) for msg in msgs_raw])

        self.register(self.socket, recv_socket)

    def send_async(self, ident, *msgs):
        for msg in msgs:
            logger.debug("Send update:     %s", msg)
        self.socket.send_msgs(ident, msgs)

    def terminate(self):
        for socket in list(self.poller.sockets):
            self.poller.unregister(socket)

    def register(self, socket, cb):
        self.poller.register(socket, zmq.POLLIN, cb)

    def unregister(self, socket):
        self.poller.unregister(socket)

    def run(self):
        # Signal actor successfully initialized
        self.evt_pipe.signal()

        while len(self.poller.sockets) > 0:
            for _, _, handler in self.poller.poll():
                handler()


class HedgehogServer(Active):
    hedgehog_server_class = HedgehogServerActor

    def __init__(self, ctx, endpoint, handlers):
        self.ctx = ctx
        self.actor = None
        self.endpoint = endpoint
        self.handlers = handlers
        self._socket = None

    @property
    def cmd_pipe(self):
        return self.actor.cmd_pipe

    @property
    def evt_pipe(self):
        return self.actor.evt_pipe

    @property
    def socket(self):
        if not self._socket:
            self.cmd_pipe.send(b'SOCK')
            self.cmd_pipe.wait()
            self._socket = self.cmd_pipe.pop()
        return self._socket

    def register(self, socket, cb):
        self.cmd_pipe.push((socket, cb))
        self.cmd_pipe.send(b'REG')

    def unregister(self, socket):
        self.cmd_pipe.push(socket)
        self.cmd_pipe.send(b'UNREG')

    def start(self):
        self.actor = Actor(self.ctx, self.hedgehog_server_class, self.endpoint, self.handlers)

    def stop(self):
        if self.actor is not None:
            self.actor.destroy()
            self.actor = None
            self._socket = None
