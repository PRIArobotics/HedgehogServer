import pytest
import asyncio.selector_events
from aiostream import stream, streamcontext
from aiostream.context_utils import async_context_manager

from hedgehog.server.subscription import SubscriptionHandler, polling_subscription_input


async def make_stream(pairs):
    for delay, item in pairs:
        await asyncio.sleep(delay)
        yield item


@async_context_manager
async def do_stream(subs, _stream):
    async def the_stream():
        async with streamcontext(_stream) as streamer:
            async for item in streamer:
                await subs.send(item)
        await subs.close()

    task = asyncio.ensure_future(the_stream())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def assert_stream(expected, _stream):
    async with stream.enumerate(_stream).stream() as streamer:
        i = -1
        async for i, item in streamer:
            exp = expected[i]
            if isinstance(exp, set):
                assert item in exp
            else:
                assert item == exp
        assert i == len(expected) - 1


@pytest.mark.asyncio
async def test_subscription_stream():
    actual = [0, 1, 2, 3, 4, 5, 6, 7]
    expected = [0, 1, {2, 3}, 4, {5, 6}, 7]

    subs = SubscriptionHandler()
    async with do_stream(subs, make_stream([(0.02, item) for item in actual])):
        await assert_stream(
            expected,
            subs.subscribe(0.03, None, None))


@pytest.mark.asyncio
async def test_subscription_stream_granularity():
    actual = [0, 1, 2, 1, 2, 1, 1, 0]
    expected = [0, 2, 1, 0]

    subs = SubscriptionHandler()
    async with do_stream(subs, make_stream([(0.02, item) for item in actual])):
        await assert_stream(
            expected,
            subs.subscribe(0.03, lambda a, b: abs(a - b) > 1, 0.09))


@pytest.mark.asyncio
async def test_subscription_stream_delayed_subscribe():
    actual = [0, 1, 2, 3, 4, 5, 6, 7]
    expected = [2, 3, {4, 5}, 6, 7]

    subs = SubscriptionHandler()
    async with do_stream(subs, make_stream([(0.02, item) for item in actual])):
        await asyncio.sleep(0.05)
        await assert_stream(
            expected,
            subs.subscribe(0.03, None, None))


@pytest.mark.asyncio
async def test_subscription_stream_cancel():
    actual = [0, 1, 2, 3, 4, 5, 6, 7]
    expected = [0, 1, {2, 3}]

    subs = SubscriptionHandler()
    async with do_stream(subs, make_stream([(0.02, item) for item in actual])):
        await assert_stream(
            expected,
            streamcontext(subs.subscribe(0.03, None, None))[:3])


@pytest.mark.asyncio
async def test_polling_subscription_input():
    i = 0

    async def poll():
        nonlocal i
        i += 1
        return i

    queue = asyncio.Queue()
    await queue.put(0.01)
    await assert_stream(
        [1, 2, 3],
        polling_subscription_input(poll, queue)[:3])

