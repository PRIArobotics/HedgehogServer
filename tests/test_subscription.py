from typing import Tuple

import pytest
from hedgehog.utils.test_utils import assertPassed

import trio
import trio_asyncio
from aiostream import streamcontext
from contextlib import asynccontextmanager

from hedgehog.server.subscription import BroadcastChannel, subscription_transform, SubscriptionStreamer


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


async def stream(*items: Tuple[any, float]):
    for value, delay in items:
        await trio.sleep(delay)
        yield value


class Stream:
    def __init__(self, stream):
        self.stream = stream
        self.index = -1

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stream.aclose()

    async def _result(self):
        begin = trio.current_time()
        try:
            exit, value = False, await self.stream.__anext__()
        except StopAsyncIteration:
            exit, value = True, None
        end = trio.current_time()
        self.index += 1
        return self.index, exit, end - begin, value

    async def expect_after(self, delay, value):
        index, exit, duration, actual = await self._result()
        if exit:
            assert False, f"#{index}: exit after {duration}s; expected: {value} after {delay}s"
        else:
            if isinstance(value, set):
                assert actual in value and duration == delay, \
                    f"#{index}: {actual} after {duration}s; expected: one of {value} after {delay}s"
            else:
                assert actual == value and duration == delay, \
                    f"#{index}: {actual} after {duration}s; expected: {value} after {delay}s"

    async def expect_exit_after(self, delay):
        index, exit, duration, actual = await self._result()
        if exit:
            assert duration == delay, f"#{index}: exit after {duration}s; expected: exit after {delay}s"
        else:
            assert False, f"#{index}: {actual} after {duration}s; expected: exit after {delay}s"

    async def expect(self, value):
        index, exit, duration, actual = await self._result()
        if exit:
            assert False, f"#{index}: exit after {duration}s; expected: {value}"
        else:
            if isinstance(value, set):
                assert actual in value, f"#{index}: {actual} after {duration}s; expected: one of {value}"
            else:
                assert actual == value, f"#{index}: {actual} after {duration}s; expected: {value}"

    async def expect_exit(self):
        index, exit, duration, actual = await self._result()
        if not exit:
            assert False, f"#{index}: {actual} after {duration}s; expected: exit"


@asynccontextmanager
async def do_stream(items):
    async with trio.open_nursery() as nursery:
        subs = SubscriptionStreamer()

        @nursery.start_soon
        async def the_stream():
            async with streamcontext(stream(*items)) as streamer:
                async for item in streamer:
                    await subs.send(item)
            await subs.close()

        yield subs
        nursery.cancel_scope.cancel()


async def assert_stream(tim_seq, out_seq, _stream):
    assert len(out_seq) == len(tim_seq)

    async with Stream(_stream) as s:
        for delay, value in zip(tim_seq, out_seq):
            await s.expect_after(delay, value)
        await s.expect_exit()


@pytest.mark.trio
async def test_broadcast_channel(autojump_clock):
    with assertPassed(2):
        async with BroadcastChannel() as broadcast, trio.open_nursery() as nursery:
            receive_a = broadcast.add_receiver(10)
            receive_b = broadcast.add_receiver(10)

            @nursery.start_soon
            async def sender():
                async for value in stream((1, 1), (2, 1)):
                    await broadcast.send(value)
                # if this is not closed here, the receiver tasks won't finish,
                # the nursery won't finish, and thus the BroadcastChannel won't exit on its own
                await broadcast.aclose()

            @nursery.start_soon
            async def receiver_a():
                async with Stream(receive_a) as s:
                    await s.expect_after(1, 1)
                    await s.expect_after(1, 2)
                    # wait for the stream to end
                    await s.expect_exit_after(0)

            @nursery.start_soon
            async def receiver_b():
                async with Stream(receive_b) as s:
                    await s.expect_after(1, 1)
                    # kill the receiver, causes it to be removed from the broadcast channel


@pytest.mark.trio
async def test_broadcast_channel_buffer_full(autojump_clock):
    with assertPassed(3):
        async with BroadcastChannel() as broadcast, trio.open_nursery() as nursery:
            receive_a = broadcast.add_receiver(1)
            receive_b = broadcast.add_receiver(1)
            receive_c = broadcast.add_receiver(2)

            @nursery.start_soon
            async def sender():
                with assertPassed(0):
                    await broadcast.send(1)
                    # time: 0
                    # A: 1, B: 1, C: 1    # after sending
                    # A: 1, B: 1, C: _    # after immediate reading
                with assertPassed(2):
                    await broadcast.send(2)
                    # A: 1!, B: 1!, C: 2   # after sending: A, B are full
                    # A: 1!, B: 1!, C: _   # after immediate reading
                    # time: 1
                    # A: 2, B: 1!, C: 2    # A has read; sending to A finishes
                    # A: _, B: 1!, C: 2    # after immediate reading
                    # time: 2
                    # A: _, B: 2, C: 2     # B has read; sending to B finishes
                    # A: _, B: _, C: 2     # after immediate reading
                with assertPassed(0):
                    await broadcast.send(3)
                    # A: 3, B: 3, C: 23    # after sending: A, B are full
                    # A: _, B: _, C: 23    # after immediate reading
                with assertPassed(1):
                    await broadcast.send(4)
                    # A: 4, B: 4, C: 23!   # after sending: C full
                    # A: _, B: _, C: 23!   # after immediate reading
                    # time: 3
                    # A: _, B: _, C: 34    # C has read; sending to C finishes
                    # A: _, B: _, C: 4     # after immediate reading
                    # A: _, B: _, C: _     # after immediate reading
                # if this is not closed here, the receiver tasks won't finish,
                # the nursery won't finish, and thus the BroadcastChannel won't exit on its own
                await broadcast.aclose()

            @nursery.start_soon
            async def receiver_a():
                async with Stream(receive_a) as s:
                    await trio.sleep(1)
                    await s.expect_after(0, 1)
                    await s.expect_after(0, 2)
                    await s.expect_after(1, 3)
                    await s.expect_after(0, 4)

                    # wait for the stream to end
                    await s.expect_exit_after(1)

            @nursery.start_soon
            async def receiver_b():
                async with Stream(receive_b) as s:
                    await trio.sleep(2)
                    await s.expect_after(0, 1)
                    await s.expect_after(0, 2)
                    await s.expect_after(0, 3)
                    await s.expect_after(0, 4)

                    # wait for the stream to end
                    await s.expect_exit_after(1)

            @nursery.start_soon
            async def receiver_c():
                async with Stream(receive_c) as s:
                    await s.expect_after(0, 1)
                    await trio.sleep(3)
                    await s.expect_after(0, 2)
                    await s.expect_after(0, 3)
                    await s.expect_after(0, 4)

                    # wait for the stream to end
                    await s.expect_exit_after(0)


@pytest.mark.trio
async def test_subscription_transform(autojump_clock):
    def stream_helper(items, timeout=None, granularity=None, granularity_timeout=None):
        in_stream = stream(*items)
        out_stream = subscription_transform(
            in_stream, timeout=timeout, granularity=granularity, granularity_timeout=granularity_timeout)
        return Stream(out_stream)

    # test that empty streams are handled properly
    async with stream_helper([]) as s:
        await s.expect_exit_after(0)

    # test subscription_transform behavior, taking into account that slow consumers must not disrupt the stream
    async with stream_helper((x, 10) for x in [0, 0, 1, 0, 1, 0, 1, 1, 0, 0]) as s:
        # this won't work, because at this point the generator has not started running,
        # and consequently the task that is reading from the stream in the background has neither
        # await trio.sleep(5)
        # await s.expect_after(5, 0)
        # instead do a regular wait in the beginning
        await s.expect_after(10, 0)
        # no value is skipped, but the next wait time is shorter because the stream produces values in the background
        await trio.sleep(5)
        await s.expect_after(15, 1)
        # one value arrived in the meantime; since it's different, the value is delivered immediately
        await trio.sleep(15)
        await s.expect_after(0, 0)
        await s.expect_after(5, 1)
        # a zero was completely skipped over and discarded; the next value is a one, so not different from before
        await trio.sleep(25)
        await s.expect_after(15, 0)
        await s.expect_exit_after(10)

    async with stream_helper(((x, 10) for x in [0, 0, 0, 0, 1, 0, 1, 0, 0, 0]),
                             timeout=31) as s:
        await s.expect_after(10, 0)
        await trio.sleep(25)
        await s.expect_after(15, 1)
        await s.expect_after(31, 0)
        await s.expect_exit_after(19)

    async with stream_helper(((x, 10) for x in [0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0]),
                             timeout=31) as s:
        await s.expect_after(10, 0)
        await trio.sleep(55)
        await s.expect_after(5, 1)
        await s.expect_after(31, 0)
        await s.expect_exit_after(19)

    async with stream_helper(((x, 10) for x in [0, 0, 0, 0, 1, 0, 1, 1, 0, 0]),
                             granularity_timeout=31) as s:
        await s.expect_after(10, 0)
        await trio.sleep(35)
        await s.expect_after(0, 0)
        await s.expect_after(5, 1)
        await trio.sleep(25)
        await s.expect_after(6, 1)
        await s.expect_after(9, 0)
        await s.expect_exit_after(10)

    async with stream_helper(((x, 10) for x in [0, 0, 0, 0, 1, 0, 1, 1, 0, 0]),
                             timeout=21, granularity_timeout=31) as s:
        await s.expect_after(10, 0)
        await trio.sleep(25)
        await s.expect_after(6, 0)
        await trio.sleep(35)
        await s.expect_after(0, 1)
        await s.expect_after(21, 0)
        await s.expect_exit_after(3)

    async with stream_helper(((x, 10) for x in [0, 1, 1, 0, 2, 0, 3, 2, 1, 4, 4]),
                             granularity=lambda a, b: abs(a - b) >= 2) as s:
        await s.expect_after(10, 0)
        await s.expect_after(40, 2)
        await s.expect_after(10, 0)
        await s.expect_after(10, 3)
        await s.expect_after(20, 1)
        await s.expect_after(10, 4)
        await s.expect_exit_after(10)

    async with stream_helper(((x, 10) for x in [0, 1, 1, 0, 2, 1, 3, 2, 1, 4, 4]),
                             granularity=lambda a, b: abs(a - b) >= 2, granularity_timeout=21) as s:
        await s.expect_after(10, 0)
        await s.expect_after(21, 1)
        await s.expect_after(21, 2)
        await s.expect_after(21, 3)
        await s.expect_after(17, 1)
        await s.expect_after(10, 4)
        await s.expect_exit_after(10)

    async with stream_helper(((x, 10) for x in [0, 1, 1, 0, 2, 0, 4, 2, 1, 4, 2, 3, 3]),
                             timeout=21, granularity=lambda a, b: abs(a - b) >= 2) as s:
        await s.expect_after(10, 0)
        await s.expect_after(40, 2)
        await s.expect_after(21, 4)
        await s.expect_after(21, 1)
        await s.expect_after(28, 3)
        await s.expect_exit_after(10)

    async with stream_helper(((x, 10) for x in [0, 1, 1, 0, 1, 0, 4, 2, 1, 4, 2, 3, 3]),
                             timeout=21, granularity=lambda a, b: abs(a - b) >= 2, granularity_timeout=41) as s:
        await s.expect_after(10, 0)
        await s.expect_after(41, 1)
        await s.expect_after(21, 4)
        await s.expect_after(21, 1)
        await s.expect_after(27, 3)
        await s.expect_exit_after(10)


@pytest.mark.trio
async def test_subscription_streamer(trio_aio_loop, autojump_clock):
    in_seq = [(value, 10) for value in range(0, 7+1)]
    out_seq = [0, 1, {2, 3}, 4, {5, 6}, 7]
    tim_seq = [10, 15, 15, 15, 15, 15]

    async with do_stream(in_seq) as subs:
        with assertPassed(sum(tim_seq)):
            await assert_stream(
                tim_seq, out_seq,
                subs.subscribe(15, None, None))


@pytest.mark.trio
async def test_subscription_streamer_granularity(autojump_clock):
    in_seq = [(value, 10) for value in [0, 1, 2, 1, 2, 1, 1, 0]]
    out_seq = [0, 2, 1, 0]
    tim_seq = [10, 20, 45, 45]

    async with do_stream(in_seq) as subs:
        with assertPassed(sum(tim_seq)):
            await assert_stream(
                tim_seq, out_seq,
                subs.subscribe(15, lambda a, b: abs(a - b) > 1, 45))


@pytest.mark.trio
async def test_subscription_streamer_delayed_subscribe(autojump_clock):
    in_seq = [(value, 10) for value in range(0, 7+1)]
    out_seq = [2, 3, {4, 5}, 6, 7]
    tim_seq = [5, 15, 15, 15, 15]

    async with do_stream(in_seq) as subs:
        await trio.sleep(25)
        with assertPassed(sum(tim_seq)):
            await assert_stream(
                tim_seq, out_seq,
                subs.subscribe(15, None, None))


@pytest.mark.trio
async def test_subscription_streamer_cancel(autojump_clock):
    in_seq = [(value, 10) for value in range(0, 7+1)]
    out_seq = [0, 1, {2, 3}]
    tim_seq = [10, 15, 15]

    async with do_stream(in_seq) as subs:
        async def the_stream():
            s = subs.subscribe(15, None, None)
            for i in range(3):
                yield await s.__anext__()
            await s.aclose()

        with assertPassed(sum(tim_seq)):
            await assert_stream(
                tim_seq, out_seq,
                the_stream())
