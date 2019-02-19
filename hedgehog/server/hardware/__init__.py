from typing import Tuple

from contextlib import AsyncExitStack
from dataclasses import dataclass
import trio

from hedgehog.protocol.errors import UnsupportedCommandError
from hedgehog.protocol import messages
from hedgehog.protocol.messages import io, analog, digital, servo
from hedgehog.protocol.messages.motor import POWER


class HardwareUpdate:
    pass


@dataclass
class MotorStateUpdate(HardwareUpdate):
    port: int
    state: int


class HardwareAdapter(object):
    def __init__(self) -> None:
        self._send_channel, self.hardware_updates = trio.open_memory_channel(10)
        self._stack: AsyncExitStack = None

    async def __aenter__(self):
        self._stack = AsyncExitStack()
        await self._stack.enter_async_context(self._send_channel)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._stack.__aexit__(exc_type, exc_val, exc_tb)

    def _enqueue_update(self, update: HardwareUpdate):
        try:
            # try enqueueing the update
            self._send_channel.send_nowait(update)
        except trio.WouldBlock:  # pragma: nocover
            try:
                # the queue is full, so this must work
                self.hardware_updates.receive_nowait()
            except trio.WouldBlock:
                assert False
            try:
                # now the queue can't be full, try again
                self._send_channel.send_nowait(update)
            except trio.WouldBlock:
                assert False

    async def set_io_state(self, port: int, flags: int) -> None:
        raise UnsupportedCommandError(messages.io.Action.msg_name())

    async def get_analog(self, port: int) -> int:
        raise UnsupportedCommandError(messages.analog.Request.msg_name())

    async def get_digital(self, port: int) -> bool:
        raise UnsupportedCommandError(messages.digital.Request.msg_name())

    async def set_motor(self, port: int, state: int, amount: int=0,
                  reached_state: int=POWER, relative: int=None, absolute: int=None) -> None:
        raise UnsupportedCommandError(messages.motor.Action.msg_name())

    async def get_motor(self, port: int) -> Tuple[int, int]:
        raise UnsupportedCommandError(messages.motor.StateRequest.msg_name())

    def motor_state_update(self, port: int, state: int) -> None:
        self._enqueue_update(MotorStateUpdate(port, state))

    async def set_motor_position(self, port: int, position: int) -> None:
        raise UnsupportedCommandError(messages.motor.SetPositionAction.msg_name())

    async def set_servo(self, port: int, active: bool, position: int) -> None:
        raise UnsupportedCommandError(messages.servo.Action.msg_name())
