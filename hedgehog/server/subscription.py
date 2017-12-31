from typing import cast, AsyncIterator, Awaitable, Callable, Dict, Generic, List, Optional, Set, Tuple, TypeVar, Union

import asyncio
import functools
from aiostream import pipe, stream, streamcontext

from hedgehog.protocol import Header
from hedgehog.protocol.proto.subscription_pb2 import Subscription
from hedgehog.protocol.errors import FailedCommandError
from hedgehog.utils.asyncio import stream_from_queue

from .hedgehog_server import HedgehogServer


T = TypeVar('T')
Upd = TypeVar('Upd')


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
        self._queues = []  # type: List[asyncio.Queue]

    async def send(self, item: T) -> None:
        for queue in self._queues:
            await queue.put(item)

    async def close(self) -> None:
        for queue in self._queues:
            await queue.put(self._EOF)

    def subscribe(self, timeout: float=None,
                  granularity: Callable[[T, T], bool]=None, granularity_timeout: float=None) -> AsyncIterator[T]:
        def sleep(timeout: Optional[float]) -> Optional[asyncio.Future]:
            return asyncio.ensure_future(asyncio.sleep(timeout)) if timeout is not None else None

        if granularity is None:
            granularity = lambda a, b: a != b

        queue = asyncio.Queue()  # type: asyncio.Queue
        self._queues.append(queue)

        async def _stream() -> AsyncIterator[T]:
            t_item = asyncio.ensure_future(queue.get())  # type: Optional[asyncio.Future]
            t_timeout = None  # type: Optional[asyncio.Future]
            t_granularity_timeout = None  # type: Optional[asyncio.Future]

            old_value = None  # type: Optional[Tuple[T]]
            new_value = None  # type: Optional[Tuple[T]]

            try:
                while t_item is not None or (new_value is not None and
                                             (t_timeout is not None or t_granularity_timeout is not None)):
                    done, pending = await asyncio.wait(
                        [t for t in (t_item, t_timeout, t_granularity_timeout) if t is not None],
                        return_when=asyncio.FIRST_COMPLETED)

                    if t_item in done:
                        result = t_item.result()
                        if result is not self._EOF:
                            new_value = (result,)
                            t_item = asyncio.ensure_future(queue.get())
                        else:
                            t_item = None

                    if t_timeout in done:
                        t_timeout = None

                    if t_granularity_timeout in done:
                        t_granularity_timeout = None

                    if new_value is not None and t_timeout is None:
                        granularity_check = old_value is None or granularity(old_value[0], new_value[0])
                        granularity_timeout_check = granularity_timeout is not None and t_granularity_timeout is None
                        if granularity_check or granularity_timeout_check:
                            if t_granularity_timeout is not None:
                                t_granularity_timeout.cancel()
                            t_timeout = sleep(timeout)
                            t_granularity_timeout = sleep(granularity_timeout)

                            yield new_value[0]
                            old_value = new_value
                            new_value = None
            finally:
                for t in (t_item, t_timeout, t_granularity_timeout):
                    if t is not None:
                        t.cancel()
                self._queues.remove(queue)

        return _stream()


def polling_subscription_input(poll: Callable[[], Union[T, Awaitable[T]]], interval_queue: asyncio.Queue) -> AsyncIterator[T]:
    """
    Returns a stream useful for poll based subscriptions.
    To support subscriptions of different frequencies, either the polling interval needs to be pessimistically small,
    or slower-than-promised updates must be accepted, or the polling interval needs to be flexible.

    `polling_subscription_input` implements flexible polling timeouts.
    The polling function may be asynchronous and returns a single value for the stream,
    while the `interval_queue` is given intervals in which to perform the polling.
    For example, by `put`ting `1` into the queue, the poll function will be subsequently called once per second,
    by later `put`ting `2` into the queue, that interval is increased to two seconds.

    An interval of zero means no timeout between `poll` calls, negative values pause polling.
    No polling is also the default before any interval was `put` into the queue yet.
    """
    return cast(AsyncIterator[T], stream_from_queue(interval_queue) | pipe.switchmap(
        lambda interval: stream.never() if interval < 0 else stream.repeat((), interval=interval) | pipe.starmap(poll)))


class Subscribable(Generic[T, Upd]):
    def __init__(self) -> None:
        self.streamer = SubscriptionStreamer[T]()
        self.subscriptions = {}  # type: Dict[Header, SubscriptionHandle]

    def compose_update(self, server: HedgehogServer, ident: Header, subscription: Subscription, value: T) -> Upd:
        raise NotImplementedError()  # pragma: no cover

    async def subscribe(self, server: HedgehogServer, ident: Header, subscription: Subscription) -> None:
        raise NotImplementedError()  # pragma: no cover


class SubscriptionHandle(object):
    def __init__(self, do_subscribe: Callable[[], Awaitable[AsyncIterator[Awaitable[None]]]]) -> None:
        self._do_subscribe = do_subscribe
        self.count = 0
        self._updates = None  # type: AsyncIterator[Awaitable[None]]

    async def increment(self) -> None:
        if self.count == 0:
            self._updates = await self._do_subscribe()
            # await self._do_subscribe()
        self.count += 1

    async def decrement(self) -> None:
        self.count -= 1
        if self.count == 0:
            await self._updates.aclose()  # type: ignore
            self._updates = None


class TriggeredSubscribable(Subscribable[T, Upd]):
    """
    Represents a value that changes by triggers known to the server, so it doesn't need to be actively polled.
    """

    def __init__(self) -> None:
        super(TriggeredSubscribable, self).__init__()

    async def update(self, value: T) -> None:
        await self.streamer.send(value)

    async def subscribe(self, server: HedgehogServer, ident: Header, subscription: Subscription) -> None:
        # TODO incomplete
        key = (ident, subscription.timeout)

        if subscription.subscribe:
            if key not in self.subscriptions:
                async def do_subscribe() -> AsyncIterator[Awaitable[None]]:
                    updates = streamcontext(self.streamer.subscribe(subscription.timeout / 1000))
                    updates |= pipe.map(lambda value:
                                        server.send_async(ident, self.compose_update(server, ident, subscription, value)))
                    updates = streamcontext(updates)
                    await server.register(updates)
                    return cast(AsyncIterator[Awaitable[None]], updates)
                handle = self.subscriptions[key] = SubscriptionHandle(do_subscribe)

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
        self.intervals = asyncio.Queue()  # type: asyncio.Queue
        self.timeouts = set()  # type: Set[float]
        self._registered = False

    async def poll(self) -> T:
        raise NotImplementedError()  # pragma: no cover

    async def register(self, server: HedgehogServer) -> None:
        if not self._registered:
            async def do_poll() -> None:
                await self.streamer.send(await self.poll())
            # do_poll is wrapped in partial so that it's treated as a regular (not async) function;
            # we want to yield the awaitable, not its result
            await server.register(polling_subscription_input(functools.partial(do_poll), self.intervals))
            self._registered = True

    async def subscribe(self, server: HedgehogServer, ident: Header, subscription: Subscription) -> None:
        await self.register(server)

        # TODO incomplete
        key = (ident, subscription.timeout)

        if subscription.subscribe:
            if key not in self.subscriptions:
                async def do_subscribe() -> AsyncIterator[Awaitable[None]]:
                    updates = streamcontext(self.streamer.subscribe(subscription.timeout / 1000))
                    updates |= pipe.map(lambda value:
                                        server.send_async(ident, self.compose_update(server, ident, subscription, value)))
                    updates = streamcontext(updates)
                    await server.register(updates)

                    self.timeouts.add(subscription.timeout / 1000)
                    await self.intervals.put(min(self.timeouts))

                    return cast(AsyncIterator[Awaitable[None]], updates)

                handle = self.subscriptions[key] = SubscriptionHandle(do_subscribe)
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
                    self.timeouts.remove(subscription.timeout / 1000)
                    await self.intervals.put(min(self.timeouts, default=-1))

                    del self.subscriptions[key]
