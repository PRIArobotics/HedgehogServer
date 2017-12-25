import asyncio
from aiostream import pipe, stream

from hedgehog.utils.asyncio import stream_from_queue


class SubscriptionHandler(object):
    """
    `SubscriptionHandler` implements the behavior regarding timeout, granularity, and granularity timeout
    described in subscription.proto.

    SubscriptionHandler receives updates via `send` and `close`
    and forwards them to all output streams created with `subscribe`, if there are any.
    Each output stream then assesses whether and when to yield the update value, according to its parameters.

    A closed output stream will no longer receive items, and when `close` is called,
    all output streams will eventually terminate as well.
    """

    _EOF = object()

    def __init__(self):
        self._queues = []

    async def send(self, item):
        for queue in self._queues:
            await queue.put(item)

    async def close(self):
        for queue in self._queues:
            await queue.put(self._EOF)

    def subscribe(self, timeout=None, granularity=None, granularity_timeout=None):
        def sleep(timeout):
            return asyncio.ensure_future(asyncio.sleep(timeout)) if timeout is not None else None

        queue = asyncio.Queue()
        self._queues.append(queue)

        async def _stream():
            t_item = asyncio.ensure_future(queue.get())
            t_timeout = None
            t_granularity_timeout = None

            old_value = None
            new_value = None

            try:
                while t_item is not None or t_timeout is not None or t_granularity_timeout is not None:
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
                        granularity_check = granularity is None or old_value is None or granularity(old_value[0], new_value[0])
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


def polling_subscription_input(poll, interval_queue):
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
    return stream_from_queue(interval_queue) | pipe.switchmap(
        lambda interval: stream.never() if interval < 0 else stream.repeat((), interval=interval) | pipe.starmap(poll))
