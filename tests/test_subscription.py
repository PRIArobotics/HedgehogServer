from typing import Tuple

import pytest
from hedgehog.utils.test_utils import assertPassed

import trio
import trio_asyncio
from aiostream import stream, streamcontext
from contextlib import asynccontextmanager

from hedgehog.server.subscription import SubscriptionStreamer


@pytest.fixture
def trio_aio_loop():
    loop = None

    @asynccontextmanager
    async def open_loop():
        nonlocal loop
        if loop is None:
            async with trio_asyncio.open_loop() as loop:
                yield loop
        else:
            yield loop

    return open_loop


async def make_stream(pairs: Tuple[float, any]):
    for delay, item in pairs:
        await trio.sleep(delay)
        yield item


@asynccontextmanager
async def do_stream(subs, _stream):
    async with trio.open_nursery() as nursery:
        @nursery.start_soon
        async def the_stream():
            async with streamcontext(_stream) as streamer:
                async for item in streamer:
                    await subs.send(item)
            await subs.close()

        yield
        nursery.cancel_scope.cancel()


async def assert_stream(tim_seq, out_seq, _stream):
    assert len(out_seq) == len(tim_seq)

    async with stream.enumerate(_stream).stream() as streamer:
        i = -1
        begin = trio.current_time()
        async for i, item in streamer:
            exp = out_seq[i]
            if isinstance(exp, set):
                assert item in exp
            else:
                assert item == exp

            end = trio.current_time()
            assert end - begin == tim_seq[i]
            begin = end
        assert i == len(out_seq) - 1


@pytest.mark.trio
async def test_subscription_streamer(trio_aio_loop, autojump_clock):
    in_seq = [(2, item) for item in range(0, 7+1)]
    tim_seq = [2, 3, 3, 3, 3, 3]
    out_seq = [0, 1, {2, 3}, 4, {5, 6}, 7]

    subs = SubscriptionStreamer()
    async with do_stream(subs, make_stream(in_seq)):
        with assertPassed(sum(tim_seq)):
            await assert_stream(
                tim_seq, out_seq,
                subs.subscribe(3, None, None))


@pytest.mark.trio
async def test_subscription_streamer_granularity(autojump_clock):
    in_seq = [(2, item) for item in [0, 1, 2, 1, 2, 1, 1, 0]]
    tim_seq = [2, 4, 9, 9]
    out_seq = [0, 2, 1, 0]

    subs = SubscriptionStreamer()
    async with do_stream(subs, make_stream(in_seq)):
        with assertPassed(sum(tim_seq)):
            await assert_stream(
                tim_seq, out_seq,
                subs.subscribe(3, lambda a, b: abs(a - b) > 1, 9))


@pytest.mark.trio
async def test_subscription_streamer_delayed_subscribe(autojump_clock):
    in_seq = [(2, item) for item in range(0, 7+1)]
    tim_seq = [1, 3, 3, 3, 3]
    out_seq = [2, 3, {4, 5}, 6, 7]

    subs = SubscriptionStreamer()
    async with do_stream(subs, make_stream(in_seq)):
        await trio.sleep(5)
        with assertPassed(sum(tim_seq)):
            await assert_stream(
                tim_seq, out_seq,
                subs.subscribe(3, None, None))


@pytest.mark.trio
async def test_subscription_streamer_cancel(autojump_clock):
    in_seq = [(2, item) for item in range(0, 7+1)]
    tim_seq = [2, 3, 3]
    out_seq = [0, 1, {2, 3}]

    subs = SubscriptionStreamer()
    async with do_stream(subs, make_stream(in_seq)):
        with assertPassed(sum(tim_seq)):
            await assert_stream(
                tim_seq, out_seq,
                streamcontext(subs.subscribe(3, None, None))[:3])
