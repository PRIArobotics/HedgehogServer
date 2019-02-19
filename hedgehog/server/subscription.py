from typing import AsyncIterator, Callable, Dict, Generic, Set, TypeVar

import math
import trio
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial

from hedgehog.protocol import Header
from hedgehog.protocol.proto.subscription_pb2 import Subscription
from hedgehog.protocol.errors import FailedCommandError

from .hedgehog_server import HedgehogServer


T = TypeVar('T')
Upd = TypeVar('Upd')


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


@asynccontextmanager
async def _skipping_stream(stream: AsyncIterator[T]):
    """\
    A helper that shields an async iterable from its consumer's timing,
    and skips any values that the consumer is too slow to retrieve.

    Async iterables are only executed, like any Python generator, when it is queried (using `await __anext__()`).
    So, consumers that block between queries influence streams' timings.
    This context manager constantly queries the input stream in a task, i.e. without pauses between `__anext__` calls),
    in the process discarding any values the consumer was too slow to retrieve.
    Only one element is buffered for the consumer. Consider this order of operations:

        in: 0         # value 0 arrives on the input stream
        retrieve 0    # buffered value is retrieved by the consumer, returning immediately
        retrieve ...  # retrieving blocks
        in: 1
             ... 1    # retrieving returns
        in: 2
        in: 3         # previous value 2 is discarded
        retrieve 3
        in: 4
        in: EOF
        retrieve 4    # when EOF is reached, the last value remains in the buffer
        retrieve EOF

    Cancelling `await __anext__()` raises `Cancelled` into the iterator, thus terminating it.
    Therefore, another interface is used: the context manager resolves to a function that returns a tuple
    `(value, eof)`; either `(<value>, False)` or `(None, True)`.
    On exiting the context manager, the input stream is closed, thus cleaning up properly. Usage:

        async with __skipping_stream(stream) as anext:
            value, eof = await anext()
            while not eof:
                foo(value)
                value, eof = await anext()
    """

    async with trio.open_nursery() as nursery:
        # has the input stream emitted a value, or reached EOF, since last looking?
        news = trio.Event()
        # has there been a value from the input stream since last emitting one to the consumer?
        has_value = False
        # has the input stream reached EOF? This may be the case even if `has_value` is also true.
        eof = False
        # what's the last value emitted by the input stream? Only valid if `has_value` is true.
        value = None

        @nursery.start_soon
        async def reader():
            nonlocal has_value, eof, value
            try:
                async for value in stream:
                    # there's a value available in `value`
                    has_value = True
                    news.set()
                eof = True
                news.set()
            finally:
                if hasattr(stream, 'aclose'):
                    await stream.aclose()
                else:  # pragma: nocover
                    pass

        async def anext():
            nonlocal has_value, eof, value

            # If `eof`, there will be no events. `has_value` may be true or not, but any input is done.
            # If `not eof`, there *will* be more events.
            # - if `has_value` is set, the event will also be set. In that case waiting is a no-op and thus fine
            # - if `has_value` is not set, then we have to wait - either for EOF or for a value
            if not eof:
                await news.wait()
                news.clear()

            # now there's news, may be a value, may be EOF (or both)
            if has_value:
                has_value = False
                return value, False
            elif eof:
                return None, True
            else:  # pragma: nocover
                assert False

        yield anext
        nursery.cancel_scope.cancel()


async def subscription_transform(stream: AsyncIterator[T], timeout: float=None,
        granularity: Callable[[T, T], bool]=None, granularity_timeout: float=None) -> AsyncIterator[T]:
    """\
    Implements the stream transformation described in `subscription.proto`.
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
    The second `1` is emitted; because at that time no timeout is active (the last emission was too long ago).
    Immediately after the emission the timeout starts again, the last `0` arrives and the input stream ends.
    Because the `0` should be emitted, the stream awaits the timeout a final time, emits the value, and then terminates.
    Had the last value been a `1`, the output stream would have terminated immediately,
    as the value would not be emitted.

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
    After the last `0`, the input ends.
    The stream waits a final time for the granularity timeout, outputs the value, and then terminates.

    Suppose the input is [0, 0] and the granularity timeout is so low that it runs out before the second zero.
    The first zero is the last value seen before the granularity timeout ran out,
    but once emitted it is out of the picture. The second zero is simply emitted as soon as it arrives.
    """
    if timeout is None:
        timeout = -math.inf
    if granularity is None:
        granularity = lambda a, b: a != b
    if granularity_timeout is None:
        granularity_timeout = math.inf

    async with _skipping_stream(stream) as anext:
        # we need a first value for our granularity checks
        value, eof = await anext()
        if eof:
            return

        while True:
            last_emit_at = trio.current_time()
            yield value
            if eof:
                return

            last_value = value
            has_value = False

            with trio.move_on_at(last_emit_at + granularity_timeout):
                with trio.move_on_at(last_emit_at + timeout):
                    while not eof:
                        # wait until there's news, save a value if there is
                        _value, eof = await anext()
                        if not eof:
                            # there's a value
                            value = _value
                            has_value = True

                # if we get here, either the timeout ran out or EOF was reached
                if eof:
                    if not has_value:
                        # no value at all
                        return
                    elif granularity(last_value, value):
                        # a good value! Wait for the timeout, then emit that value
                        await trio.sleep_until(last_emit_at + timeout)
                        continue
                    elif granularity_timeout < math.inf:
                        # there's still a chance to send the value after the granularity timeout
                        await trio.sleep_forever()
                    else:
                        # again, nothing to send
                        return
                    # note that none of the branches continue here

                # not EOF, so do regular waiting for values
                while not eof and (not has_value or not granularity(last_value, value)):
                    # wait until there's news, save a value if there is
                    _value, eof = await anext()
                    if not eof:
                        # there's a value
                        value = _value
                        has_value = True

                if eof:
                    # EOF was reached.
                    # If there is a value, we know that the granularity did not break the loop;
                    # no need to check that again.
                    if not has_value:
                        # no value at all
                        return
                    elif granularity_timeout < math.inf:
                        # there's still a chance to send the value after the granularity timeout
                        await trio.sleep_forever()
                    else:
                        # again, nothing to send
                        return
                    # note that none of the branches continue here

            # after the granularity timeout, we're fine with any value
            if has_value:
                continue

            # wait for the next event
            value, eof = await anext()
            if eof:
                return


class SubscriptionStreamer(Generic[T]):
    """
    `SubscriptionStreamer` implements the behavior regarding timeout, granularity, and granularity timeout
    described in subscription.proto.

    SubscriptionStreamer receives updates via `send` and `close`
    and forwards them to all output streams created with `subscribe`, if there are any.
    Each output stream then assesses whether and when to yield the update value, according to its parameters.

    A closed output stream will no longer receive items, and when `close` is called,
    all output streams will eventually terminate as well.
    """

    _EOF = object()

    def __init__(self) -> None:
        self.broadcast = BroadcastChannel()

    async def send(self, item: T) -> None:
        await self.broadcast.send(item)

    async def close(self) -> None:
        await self.broadcast.aclose()

    def subscribe(self, timeout: float=None,
                  granularity: Callable[[T, T], bool]=None, granularity_timeout: float=None) -> AsyncIterator[T]:
        return subscription_transform(self.broadcast.add_receiver(10), timeout, granularity, granularity_timeout)


# TODO how to extract logic from this so that it is not coupled to the Hedgehog server?

class Subscribable(Generic[T, Upd]):
    def __init__(self) -> None:
        self.streamer = SubscriptionStreamer[T]()
        self.subscriptions = {}  # type: Dict[Header, SubscriptionHandle]

    def compose_update(self, server: HedgehogServer, ident: Header, subscription: Subscription, value: T) -> Upd:
        raise NotImplemented

    async def subscribe(self, server: HedgehogServer, ident: Header, subscription: Subscription) -> None:
        raise NotImplemented


class SubscriptionHandle(object):
    def __init__(self, server: HedgehogServer, update_sender) -> None:
        self.count = 0
        self.server = server
        self._update_sender = update_sender
        self._scope = None

    async def _update_task(self, *, task_status=trio.TASK_STATUS_IGNORED):
        with trio.CancelScope() as scope:
            task_status.started(scope)
            await self._update_sender()

    async def increment(self) -> None:
        if self._scope is not None:
            self._scope.cancel()
        self._scope = await self.server.add_task(self._update_task)
        self.count += 1

    async def decrement(self) -> None:
        self.count -= 1
        if self.count == 0:
            self._scope.cancel()
            self._scope = None


class TriggeredSubscribable(Subscribable[T, Upd]):
    """
    Represents a value that changes by triggers known to the server, so it doesn't need to be actively polled.
    """

    def __init__(self) -> None:
        super(TriggeredSubscribable, self).__init__()

    async def update(self, value: T) -> None:
        await self.streamer.send(value)

    async def _update_sender(self, server: HedgehogServer, ident: Header, subscription: Subscription):
        async for value in self.streamer.subscribe(subscription.timeout / 1000):
            async with server.job():
                await server.send_async(ident, self.compose_update(server, ident, subscription, value))

    async def subscribe(self, server: HedgehogServer, ident: Header, subscription: Subscription) -> None:
        # TODO incomplete
        key = (ident, subscription.timeout)

        if subscription.subscribe:
            if key not in self.subscriptions:
                update_sender = partial(self._update_sender, server, ident, subscription)
                handle = self.subscriptions[key] = SubscriptionHandle(server, update_sender)
            else:
                handle = self.subscriptions[key]

            await handle.increment()
        else:
            try:
                handle = self.subscriptions[key]
            except KeyError:
                raise FailedCommandError("can't cancel nonexistent subscription")
            else:
                await handle.decrement()
                if handle.count == 0:
                    del self.subscriptions[key]


class PolledSubscribable(Subscribable[T, Upd]):
    """
    Represents a value that changes by independently from the server, so it is polled to observe changes.
    """

    def __init__(self) -> None:
        super(PolledSubscribable, self).__init__()
        self.intervals: trio.abc.SendChannel = None
        self.timeouts = set()  # type: Set[float]

    @property
    def _registered(self):
        return self.intervals is not None

    async def poll(self) -> T:
        raise NotImplemented

    async def _poll_task(self, *, task_status=trio.TASK_STATUS_IGNORED):
        self.intervals, intervals = trio.open_memory_channel(0)
        task_status.started()

        async with trio.open_nursery() as nursery:
            async def poller(interval, *, task_status=trio.TASK_STATUS_IGNORED):
                with trio.CancelScope() as scope:
                    task_status.started(scope)
                    while True:
                        await self.streamer.send(await self.poll())
                        await trio.sleep(interval)

            @nursery.start_soon
            async def read_interval():
                scope = None
                async for interval in intervals:
                    # cancel the old polling task
                    if scope is not None:
                        scope.cancel()
                        scope = None

                    # start new polling task
                    if interval >= 0:
                        scope = await nursery.start(poller, interval)

    async def _update_sender(self, server: HedgehogServer, ident: Header, subscription: Subscription):
        async for value in self.streamer.subscribe(subscription.timeout / 1000):
            async with server.job():
                await server.send_async(ident, self.compose_update(server, ident, subscription, value))

    async def register(self, server: HedgehogServer) -> None:
        if not self._registered:
            await server.add_task(self._poll_task)

    async def subscribe(self, server: HedgehogServer, ident: Header, subscription: Subscription) -> None:
        await self.register(server)

        # TODO incomplete
        key = (ident, subscription.timeout)

        if subscription.subscribe:
            if key not in self.subscriptions:
                self.timeouts.add(subscription.timeout / 1000)
                update_sender = partial(self._update_sender, server, ident, subscription)
                handle = self.subscriptions[key] = SubscriptionHandle(server, update_sender)
            else:
                handle = self.subscriptions[key]

            await handle.increment()
            await self.intervals.send(min(self.timeouts))
        else:
            try:
                handle = self.subscriptions[key]
            except KeyError:
                raise FailedCommandError("can't cancel nonexistent subscription")
            else:
                await handle.decrement()
                if handle.count == 0:
                    self.timeouts.remove(subscription.timeout / 1000)
                    await self.intervals.send(min(self.timeouts, default=-1))

                    del self.subscriptions[key]
