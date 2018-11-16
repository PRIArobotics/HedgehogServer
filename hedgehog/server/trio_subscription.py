from typing import AsyncIterator, Callable, Generic, Set, Tuple, TypeVar

from contextlib import AsyncExitStack
import math
import trio

T = TypeVar('T')


class BroadcastChannel(Generic[T], trio.abc.AsyncResource):
    def __init__(self) -> None:
        self._send_channels: Set[trio.abc.SendChannel] = set()
        self._stack = AsyncExitStack()

    async def send(self, value: T) -> None:
        broken = set()

        async def send(channel):
            try:
                await channel.send(value)
            except trio.BrokenResourceError:
                await channel.aclose()
                broken.add(channel)

        async with trio.open_nursery() as nursery:
            for channel in self._send_channels:
                nursery.start_soon(send, channel)

        self._send_channels -= broken
        broken.clear()

    def add_receiver(self, max_buffer_size) -> trio.abc.ReceiveChannel:
        send, receive = trio.open_memory_channel(max_buffer_size)
        self._stack.push_async_exit(send)
        self._send_channels.add(send)
        return receive

    async def aclose(self):
        await self._stack.aclose()


async def subscription_transform(stream: AsyncIterator[T], timeout: float=None,
        granularity: Callable[[T, T], bool]=None, granularity_timeout: float=None) -> AsyncIterator[T]:
    try:
        if granularity is None:
            granularity = lambda a, b: a != b
        if granularity_timeout is None:
            granularity_timeout = math.inf

        async with trio.open_nursery() as nursery:
            # has the stream produced a value (or terminated) since last looking?
            new_value = trio.Event()
            # what's the last value produced by the stream?
            value = None

            @nursery.start_soon
            async def reader():
                nonlocal value
                async for value in stream:
                    new_value.set()
                # this may discard the last values of the stream, but that's fine
                # this makes sure that the stream terminates immediately when it's clear
                # that no more data can arrive; no pending timeouts
                nursery.cancel_scope.cancel()

            # we need a first value for our granularity checks
            await new_value.wait()
            new_value.clear()

            while True:
                # store the latest value for comparison
                # do that before publishing the value, because value could later change
                last_value = value
                last_value_at = trio.current_time()
                yield value

                # has there been a value since last publishing one?
                has_value = False

                # normal operation until the granularity timeout is reached; after that take the first value
                with trio.move_on_at(last_value_at + granularity_timeout):
                    # wait at least for the timeout before checking on sending a value
                    if timeout:
                        await trio.sleep_until(last_value_at + timeout)

                    while True:
                        # wait until there's a value
                        await new_value.wait()
                        new_value.clear()

                        # now we know there's a value
                        has_value = True
                        # should we send this value?
                        if granularity(last_value, value):
                            break

                if not has_value:
                    # we did not once observe a new value on the stream
                    # the granularity timeout is over, but we still need that one value
                    await new_value.wait()
                    new_value.clear()
                # now we know there's a value; will be published on the next iteration
    finally:
        if hasattr(stream, 'aclose'):
            await stream.aclose()
