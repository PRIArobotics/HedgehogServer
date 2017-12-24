import asyncio
from aiostream import streamcontext

from hedgehog.utils.asyncio import Active


class SubscriptionStream(Active):
    """
    `SubscriptionStream` implements the behavior regarding timeout, granularity, and granularity timeout
    described in subscription.proto.

    SubscriptionStream polls a given input `stream` asynchronously; to manage this asynchronous polling,
    this class is `Active`.
    It forwards the input items to all output streams created with `subscribe`, if there are any.
    Each subscribed stream then assesses what to do with that value according to its parameters.

    A closed output stream will no longer receive items, and when the input stream terminates,
    or the subscription stream is stopped as an `Active`, all output streams will eventually terminate as well.
    """

    _EOF = object()

    def __init__(self, stream):
        self._queues = []
        self._stream = stream
        self._poller = None

    async def start(self):
        self._poller = asyncio.ensure_future(self._input_poller(self._stream))

    async def stop(self):
        self._poller.cancel()
        try:
            await self._poller
        except asyncio.CancelledError:
            pass

    async def _input_poller(self, stream):
        try:
            async with streamcontext(stream) as streamer:
                async for item in streamer:
                    for queue in self._queues:
                        await queue.put(item)
        finally:
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
