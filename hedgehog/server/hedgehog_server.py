from typing import Any, AsyncGenerator, Callable, Coroutine, Dict, Type

import asyncio
import logging
import traceback
import zmq.asyncio
from aiostream import pipe, streamcontext
from hedgehog.utils.asyncio import Actor, stream_from_queue
from hedgehog.utils.zmq.poller import Poller
from hedgehog.utils.zmq.socket import SocketLike
from hedgehog.utils.zmq.timer import Timer
from hedgehog.protocol import ServerSide, Header, RawMessage, Message
from hedgehog.protocol.async_sockets import DealerRouterSocket
from hedgehog.protocol.errors import HedgehogCommandError, UnsupportedCommandError, FailedCommandError


# TODO importing this from .handlers does not work...
HandlerCallback = Callable[['HedgehogServer', Header, Message], Coroutine[Any, Any, Message]]

logger = logging.getLogger(__name__)


class HedgehogServer(Actor):
    def __init__(self, ctx: zmq.asyncio.Context, endpoint: str, handlers: Dict[Type[Message], HandlerCallback]) -> None:
        self.ctx = ctx
        self.endpoint = endpoint
        self.handlers = handlers
        self.socket = None  # type: DealerRouterSocket

    async def register(self, stream: AsyncGenerator[Coroutine[Any, Any, Any], Any]) -> None:
        await self.cmd_pipe.send((b'REG', stream))

    async def send_async(self, ident: Header, *msgs: Message) -> None:
        for msg in msgs:
            logger.debug("Send update:     %s", msg)
        await self.socket.send_msgs(ident, msgs)

    async def _requests(self):
        async def handle_msg(ident, msg_raw):
            try:
                msg = ServerSide.parse(msg_raw)
                logger.debug("Receive command: %s", msg)
                try:
                    handler = self.handlers[msg.__class__]
                except KeyError:
                    raise UnsupportedCommandError(msg.__class__.msg_name())
                try:
                    result = await handler(self, ident, msg)
                except HedgehogCommandError:
                    raise
                except Exception as err:
                    traceback.print_exc()
                    raise FailedCommandError("uncaught exception: {}".format(repr(err))) from err
            except HedgehogCommandError as err:
                result = err.to_message()
            logger.debug("Send reply:      %s", result)
            return ServerSide.serialize(result)

        async def request_handler(ident, msgs_raw):
            await self.socket.send_msgs_raw(ident, [await handle_msg(ident, msg) for msg in msgs_raw])

        while True:
            ident, msgs_raw = await self.socket.recv_msgs_raw()
            yield request_handler(ident, msgs_raw)

    async def run(self, cmd_pipe, evt_pipe):
        self.socket = DealerRouterSocket(self.ctx, zmq.ROUTER, side=ServerSide)
        self.socket.bind(self.endpoint)
        await evt_pipe.send(b'$START')

        stream_queue = asyncio.Queue()

        async def commands():
            while True:
                cmd = await cmd_pipe.recv()
                yield (cmd,) if isinstance(cmd, bytes) else cmd

        await stream_queue.put(commands())
        await self.register(self._requests())

        events = stream_from_queue(stream_queue) | pipe.flatten()
        async with events.stream() as streamer:
            async for cmd, *payload in streamer:
                if cmd == b'EVENT':
                    awaitable, = payload
                    await awaitable
                elif cmd == b'REG':
                    stream, = payload
                    await stream_queue.put(streamcontext(stream) | pipe.map(lambda item: (b'EVENT', item)))
                elif cmd == b'$TERM':
                    break

        self.socket.close()
