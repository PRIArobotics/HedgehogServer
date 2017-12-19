
import pytest
import asyncio.selector_events
from aiostream import stream

from hedgehog.server.subscription import SubscriptionStream


async def make_stream(pairs):
    for delay, item in pairs:
        await asyncio.sleep(delay)
        yield item


async def assert_stream(expected, _stream):
    async with stream.enumerate(_stream).stream() as streamer:
        i = -1
        async for i, item in streamer:
            exp = expected[i]
            if isinstance(exp, tuple):
                assert item in exp
            else:
                assert item == exp
        assert i == len(expected) - 1


class TestSubscription(object):
    @pytest.mark.asyncio
    async def test_subscription_stream(self, event_loop):
        actual = [0, 1, 2, 3, 4, 5, 6, 7]
        expected = [0, 1, (2, 3), 4, (5, 6), 7]

        subs = SubscriptionStream(make_stream([(0.02, item) for item in actual]))
        await assert_stream(
            expected,
            subs.subscribe(0.03, None, None))

    @pytest.mark.asyncio
    async def test_subscription_stream_granularity(self, event_loop):
        actual = [0, 1, 2, 1, 2, 1, 1, 0]
        expected = [0, 2, 1, 0]

        subs = SubscriptionStream(make_stream([(0.02, item) for item in actual]))
        await assert_stream(
            expected,
            subs.subscribe(0.03, lambda a, b: abs(a - b) > 1, 0.09))
