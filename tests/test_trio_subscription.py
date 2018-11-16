from typing import Tuple

import pytest
from hedgehog.utils.test_utils import assertPassed

import trio

from hedgehog.server.trio_subscription import BroadcastChannel, subscription_transform


async def stream(*items: Tuple[any, float]):
    for value, delay in items:
        await trio.sleep(delay)
        yield value


class Stream:
    def __init__(self, stream):
        self.stream = stream

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
        return exit, end - begin, value

    async def expect_after(self, delay, value):
        exit, duration, actual = await self._result()
        if exit:
            assert False, f"exit after {duration}s; expected: {value} after {delay}s"
        else:
            assert (duration, actual) == (delay, value), f"{actual} after {duration}s; expected: {value} after {delay}s"

    async def expect_exit_after(self, delay):
        exit, duration, actual = await self._result()
        if exit:
            assert duration == delay, f"exit after {duration}s; expected: exit after {delay}s"
        else:
            assert False, f"{actual} after {duration}s; expected: exit after {delay}s"


@pytest.mark.trio
async def test_trio_subscription_broadcast(autojump_clock):
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
async def test_trio_subscription_transform(autojump_clock):
    def stream_helper(items, timeout=None, granularity=None, granularity_timeout=None):
        in_stream = stream(*items)
        out_stream = subscription_transform(
            in_stream, timeout=timeout, granularity=granularity, granularity_timeout=granularity_timeout)
        return Stream(out_stream)

    # test that empty streams are handled properly
    async with stream_helper([]) as s:
        await s.expect_exit_after(0)

    # test that, without arguments, slow consumers don't disrupt the stream
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
