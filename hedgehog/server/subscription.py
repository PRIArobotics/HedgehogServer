from typing import AsyncIterator, Awaitable, Callable, Dict, Generic, List, Optional, Set, Tuple, TypeVar

import asyncio
import trio
from functools import partial

from hedgehog.protocol import Header
from hedgehog.protocol.proto.subscription_pb2 import Subscription
from hedgehog.protocol.errors import FailedCommandError

from .hedgehog_server import HedgehogServer
from .trio_subscription import BroadcastChannel, subscription_transform


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
        self.broadcast = BroadcastChannel()

    async def send(self, item: T) -> None:
        await self.broadcast.send(item)

    async def close(self) -> None:
        await self.broadcast.aclose()

    def subscribe(self, timeout: float=None,
                  granularity: Callable[[T, T], bool]=None, granularity_timeout: float=None) -> AsyncIterator[T]:
        return subscription_transform(self.broadcast.add_receiver(10), timeout, granularity, granularity_timeout)


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
        with trio.open_cancel_scope() as scope:
            task_status.started(scope)
            await self._update_sender()

    async def increment(self) -> None:
        if self.count == 0:
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
                with trio.open_cancel_scope() as scope:
                    task_status.started(scope)
                    while True:
                        await self.streamer.send(await self.poll())
                        await trio.sleep(interval)

            @nursery.start_soon
            async def read_interval():
                current_interval = -1
                current_scope = None
                async for interval in intervals:
                    # cancel the old polling task
                    if interval != current_interval and current_scope is not None:
                        current_scope.cancel()
                        current_scope = None

                    # start new polling task
                    current_interval = interval
                    if current_interval >= 0:
                        current_scope = await nursery.start(poller, current_interval)

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
                await self.intervals.send(min(self.timeouts))

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
                    self.timeouts.remove(subscription.timeout / 1000)
                    await self.intervals.send(min(self.timeouts, default=-1))

                    del self.subscriptions[key]
