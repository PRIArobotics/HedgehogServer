from typing import AsyncIterator, Awaitable, Callable, Dict, Type

import asyncio
import logging
import zmq.asyncio
from aiostream import pipe
from contextlib import asynccontextmanager
from functools import partial
from hedgehog.utils.asyncio import stream_from_queue
from hedgehog.protocol import ServerSide, Header, RawMessage, Message, RawPayload
from hedgehog.protocol.async_sockets import DealerRouterSocket
from hedgehog.protocol.errors import HedgehogCommandError, UnsupportedCommandError, FailedCommandError

from concurrent_utils.pipe import PipeEnd
from concurrent_utils.component import Component, component_coro_wrapper, start_component


# TODO importing this from .handlers does not work...
HandlerCallback = Callable[['HedgehogServer', Header, Message], Awaitable[Message]]
Job = Callable[[], Awaitable[None]]

logger = logging.getLogger(__name__)


class HedgehogServer:
    class Stop(BaseException): pass

    def __init__(self, ctx: zmq.asyncio.Context, endpoint: str, handlers: Dict[Type[Message], HandlerCallback]) -> None:
        self.ctx = ctx
        self.endpoint = endpoint
        self.handlers = handlers
        self.socket: zmq.asyncio.Socket = None
        self._queue = asyncio.Queue()

    async def _commands_job_stream(self, commands: PipeEnd) -> AsyncIterator[Job]:
        async def stop_server():
            raise HedgehogServer.Stop

        while True:
            command = await commands.recv()
            if command == Component.COMMAND_STOP:
                yield stop_server
            else:
                raise ValueError(f"unknown command: {command!r}")

    async def _requests_job_stream(self) -> AsyncIterator[Job]:
        async def handle_msg(ident: Header, msg_raw: RawMessage) -> RawMessage:
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
                    logger.exception("Uncaught exception in command handler")
                    raise FailedCommandError("Uncaught exception: {}".format(repr(err))) from err
            except HedgehogCommandError as err:
                result = err.to_message()
            logger.debug("Send reply:      %s", result)
            return ServerSide.serialize(result)

        async def request_handler(ident: Header, msgs_raw: RawPayload) -> None:
            await self.socket.send_msgs_raw(ident, [await handle_msg(ident, msg) for msg in msgs_raw])

        while True:
            ident, msgs_raw = await self.socket.recv_msgs_raw()
            yield partial(request_handler, ident, msgs_raw)

    def add_job_stream(self, async_iter: AsyncIterator[Job]) -> None:
        async def async_iter_wrapper() -> AsyncIterator[Job]:
            logger.debug("Added new job iterator: %s", async_iter)
            try:
                async for job in async_iter:
                    yield job
            except GeneratorExit:
                logger.debug("Job iterator was stopped: %s", async_iter)
                raise
            except Exception:
                logger.exception("Job iterator raised an exception: %s", async_iter)
            else:
                logger.debug("Job iterator finished: %s", async_iter)

        self._queue.put_nowait(async_iter_wrapper())

    async def _run_job(self, job: Job) -> None:
        LONG_RUNNING_DELAY = 0.1
        long_running = asyncio.get_event_loop().call_later(
            LONG_RUNNING_DELAY, logger.warning, "Long running job on server loop: %s", job)
        begin = asyncio.get_event_loop().time()
        try:
            await job()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Job raised an exception: %s", job)
        finally:
            long_running.cancel()
            end = asyncio.get_event_loop().time()
            if end - begin > LONG_RUNNING_DELAY:
                logger.warning("Long running job finished after %.1f ms: %s", (end - begin) * 1000, job)

    async def send_async(self, ident: Header, *msgs: Message) -> None:
        for msg in msgs:
            logger.debug("Send update:     %s", msg)
        await self.socket.send_msgs(ident, msgs)

    async def _workload(self, *, commands: PipeEnd, events: PipeEnd) -> None:
        with DealerRouterSocket(self.ctx, zmq.ROUTER, side=ServerSide) as self.socket:
            self.socket.bind(self.endpoint)
            await events.send(Component.EVENT_START)

            self._queue.put_nowait(self._commands_job_stream(commands))
            self._queue.put_nowait(self._requests_job_stream())
            jobs_stream = stream_from_queue(self._queue) | pipe.flatten()
            async with jobs_stream.stream() as streamer:
                try:
                    async for job in streamer:
                        await self._run_job(job)
                except HedgehogServer.Stop:
                    logger.info("Server stopped")

    async def workload(self, *, commands: PipeEnd, events: PipeEnd) -> None:
        return await component_coro_wrapper(self._workload, commands=commands, events=events)

    @classmethod
    @asynccontextmanager
    async def start(cls, ctx: zmq.asyncio.Context, endpoint: str, handlers: Dict[Type[Message], HandlerCallback]) -> Component[None]:
        component = await start_component(cls(ctx, endpoint, handlers).workload)
        try:
            yield component
        finally:
            await component.stop()
