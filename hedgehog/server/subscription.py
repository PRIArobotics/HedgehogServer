from typing import cast, AsyncIterator, Awaitable, Callable, Dict, Generic, List, Optional, Set, Tuple, TypeVar, Union

import asyncio
from functools import partial
from aiostream import pipe, stream, streamcontext

from hedgehog.protocol import Header
from hedgehog.protocol.proto.subscription_pb2 import Subscription
from hedgehog.protocol.errors import FailedCommandError
from hedgehog.utils.asyncio import stream_from_queue

from .hedgehog_server import HedgehogServer, EventStream


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


class Subscribable(Generic[T, Upd]):
    def __init__(self) -> None:
        self.streamer = SubscriptionStreamer[T]()
        self.subscriptions = {}  # type: Dict[Header, SubscriptionHandle]

    def compose_update(self, server: HedgehogServer, ident: Header, subscription: Subscription, value: T) -> Upd:
        raise NotImplementedError()  # pragma: no cover

    async def subscribe(self, server: HedgehogServer, ident: Header, subscription: Subscription) -> None:
        raise NotImplementedError()  # pragma: no cover


class SubscriptionHandle(object):
    def __init__(self, do_subscribe: Callable[[], Awaitable[EventStream]]) -> None:
        self._do_subscribe = do_subscribe
        self.count = 0
        self._updates = None  # type: EventStream

    async def increment(self) -> None:
        if self.count == 0:
            self._updates = await self._do_subscribe()
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
                async def do_subscribe() -> EventStream:
                    updates = streamcontext(self.streamer.subscribe(subscription.timeout / 1000))
                    updates |= pipe.map(lambda value: partial(server.send_async, ident,
                                                              self.compose_update(server, ident, subscription, value)))
                    events = cast(EventStream, streamcontext(updates))
                    await server.register(events)
                    return events
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

            events = stream_from_queue(self.intervals) | pipe.switchmap(
                lambda interval: stream.never() if interval < 0 else stream.repeat(do_poll, interval=interval))

            await server.register(events)
            self._registered = True

    async def subscribe(self, server: HedgehogServer, ident: Header, subscription: Subscription) -> None:
        await self.register(server)

        # TODO incomplete
        key = (ident, subscription.timeout)

        if subscription.subscribe:
            if key not in self.subscriptions:
                async def do_subscribe() -> EventStream:
                    updates = streamcontext(self.streamer.subscribe(subscription.timeout / 1000))
                    updates |= pipe.map(lambda value: partial(server.send_async, ident,
                                                              self.compose_update(server, ident, subscription, value)))
                    events = cast(EventStream, streamcontext(updates))
                    await server.register(events)

                    self.timeouts.add(subscription.timeout / 1000)
                    await self.intervals.put(min(self.timeouts))

                    return events

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
