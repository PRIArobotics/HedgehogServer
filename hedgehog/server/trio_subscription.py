from typing import AsyncIterator, Callable, Generic, Set, Tuple, TypeVar

from contextlib import AsyncExitStack
import math
import trio

T = TypeVar('T')


class BroadcastChannel(Generic[T], trio.abc.AsyncResource):
    """\
    Bundles a set of trio channels so that messages are sent to all of them.
    When a receiver is closed, it is cleanly removed from the broadcast channel on the next send.
    Be careful about the buffer size chosen when adding a receiver; see `send()` for details.
    """

    def __init__(self) -> None:
        self._send_channels: Set[trio.abc.SendChannel] = set()
        self._stack = AsyncExitStack()

    async def send(self, value: T) -> None:
        """\
        Sends the value to all receiver channels.
        Closed receivers are removed the next time a value.'is sent using this method.
        This method will send to all receivers immediately,
        but it will block until the message got out to all receivers.

        Suppose you have receivers A and B with buffer size zero, and you send to them:

            await channel.send(1)
            await channel.send(2)

        If only B is actually reading, then `send(2)` will not be called, because `send(1)` can't finish,
        meaning the `2` is not delivered to B either.
        To prevent this, close any receivers that are done, and/or poll receive in a timely manner.
        """
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
        """\
        Adds a receiver to this broadcast channel with the given buffer capacity.
        The send end of the receiver is closed when the broadcast channel is closed,
        and if the receive end is closed, it is discarded from the broadcast channel.
        """
        send, receive = trio.open_memory_channel(max_buffer_size)
        self._stack.push_async_exit(send)
        self._send_channels.add(send)
        return receive

    async def aclose(self):
        """\
        Closes the broadcast channel, causing all receive channels to stop iteration.
        """
        await self._stack.aclose()


async def subscription_transform(stream: AsyncIterator[T], timeout: float=None,
        granularity: Callable[[T, T], bool]=None, granularity_timeout: float=None) -> AsyncIterator[T]:
    """\
    Implements the stream transformation described `subscription.proto`.
    The identity transform would be `subscription_transform(stream, granularity=lambda a, b: True)`:
    no timing behavior is added, and all values are treated as distinct, and thus emitted.

    If `granularity` is not given, values are compared for equality, thus from `[0, 0, 2, 1, 1, 0, 0, 1]`,
    elements 1, 4, and 6 would be discarded as being duplicates of their previous values.
    A typical example granularity measure for numbers is a lower bound on value difference,
    e.g. `lambda a, b: abs(a-b) > THRESHOLD`.

    The `timeout` parameter specifies a minimum time to pass between subsequent emitted values.
    After the timeout has passed, the most recently received value (if any) will be considered
    as if it had just arrived on the input stream,
    and then all subsequent values are considered until the next emission.
    Suppose the input is [0, 1, 0, 1, 0] and the timeout is just enough to skip one value completely.
    After emitting `0`, the first `1` is skipped, and the second `0` is not emitted because it's not a new value.
    The second `1` is emitted; because at that time no timeout is active (the last emission was too long ago.
    Immediately after the emission the timeout starts again,
    ignoring the last `0`, reaching the end of the input and terminating the stream even before the timeout expired.

    The `granularity_timeout` parameter specifies a maximum time to pass between subsequent emitted values,
    as long as there were input values at all.
    The `granularity` may discard values of the input stream,
    leading in the most extreme case to no emitted values at all.
    If a `granularity_timeout` is given, then the most recent input value is emitted after that time,
    restarting both the ordinary and granularity timeout in the process.
    Suppose the input is [0, 0, 0, 1, 1, 0, 0] and the granularity timeout is just enough to skip one value completely.
    After emitting `0` and skipping the next one, another `0` is emitted:
    although the default granularity discarded the unchanged value, the granularity timeout forces its emission.
    Then, the first `1` and next `0` are emitted as normal, as changed values appeared before the timeout ran out.

    Suppose the input is [0, 0] and the granularity timeout is so low that it runs out before the second zero.
    Even though the next value (the second zero) is forced to be emitted as soon as it arrives,
    the first zero is not emitted twice.
    It is the last value seen before the granularity timeout ran out, but once emitted it is out of the picture.
    """
    try:
        if granularity is None:
            granularity = lambda a, b: a != b
        if granularity_timeout is None:
            granularity_timeout = math.inf

        async with trio.open_nursery() as nursery:
            # has the input stream emitted a value (or terminated) since last looking?
            new_value = trio.Event()
            # what's the last value emitted by the input stream?
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
                # store the latest value & time for comparison
                # do that before emitting the value, because the stream's consumer could take its time
                last_value = value
                last_value_at = trio.current_time()
                yield value

                # has there been a value from the input stream since last emitting one?
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
                    # we did not once observe a new value on the input stream
                    # the granularity timeout is over, but we still need that one value
                    await new_value.wait()
                    new_value.clear()
                # now we know there's a value; will be emitted on the next iteration
    finally:
        if hasattr(stream, 'aclose'):
            await stream.aclose()
