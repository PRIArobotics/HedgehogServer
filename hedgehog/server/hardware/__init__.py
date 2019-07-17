from typing import Tuple

from contextlib import AsyncExitStack
from dataclasses import dataclass
import trio

from hedgehog.protocol.errors import UnsupportedCommandError
from hedgehog.protocol import messages
from hedgehog.protocol.messages import version, emergency, io, analog, imu, digital, motor, servo, speaker
from hedgehog.protocol.messages.motor import POWER


class HardwareUpdate:
    pass


@dataclass
class ShutdownUpdate(HardwareUpdate):
    pass


@dataclass
class EmergencyStopUpdate(HardwareUpdate):
    pass


@dataclass
class MotorStateUpdate(HardwareUpdate):
    port: int
    state: int


@dataclass
class UartUpdate(HardwareUpdate):
    data: bytes


class HardwareAdapter(object):
    def __init__(self, *, update_buffer_size=10) -> None:
        self._updates_in, self._updates_out = trio.open_memory_channel(update_buffer_size)
        self._stack: AsyncExitStack = None

    async def __aenter__(self):
        self._stack = AsyncExitStack()
        await self._stack.enter_async_context(self._updates_in)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._stack.__aexit__(exc_type, exc_val, exc_tb)

    def _enqueue_update(self, update: HardwareUpdate):
        """
        Adds an update from the hardware to a queue.
        This is normally done from inside the hardware adapter itself.
        The updates can be retrieved from `self.updates`.

        If the queue is full, the oldest update in the queue is discarded,
        so it is (relatively) safe to ignore the queue when no hardware updates are needed.
        """
        try:
            # try enqueueing the update
            self._updates_in.send_nowait(update)
        except trio.WouldBlock:  # pragma: nocover
            try:
                # the queue is full, so this must work
                # consume the least recent update in the queue
                self._updates_out.receive_nowait()
            except trio.WouldBlock:
                assert False
            try:
                # now the queue can't be full, try again
                self._updates_in.send_nowait(update)
            except trio.WouldBlock:
                assert False

    @property
    def updates(self) -> trio.abc.ReceiveChannel:
        """
        A queue of updates from the hardware.
        Old updates are discarded if the queue is not consumed quickly enough,
        so it is (relatively) safe to ignore the queue when no hardware updates are needed.
        """
        return self._updates_out

    async def get_version(self) -> Tuple[bytes, int, int]:
        raise UnsupportedCommandError(messages.version.Request.msg_name())

    async def emergency_action(self, activate) -> None:
        raise UnsupportedCommandError(messages.emergency.Action.msg_name())

    async def get_emergency_state(self) -> bool:
        raise UnsupportedCommandError(messages.emergency.Request.msg_name())

    async def set_io_config(self, port: int, flags: int) -> None:
        raise UnsupportedCommandError(messages.io.Action.msg_name())

    async def get_analog(self, port: int) -> int:
        raise UnsupportedCommandError(messages.analog.Request.msg_name())

    async def get_imu_rate(self) -> Tuple[int, int, int]:
        raise UnsupportedCommandError(messages.imu.RateRequest.msg_name())

    async def get_imu_acceleration(self) -> Tuple[int, int, int]:
        raise UnsupportedCommandError(messages.imu.AccelerationRequest.msg_name())

    async def get_imu_pose(self) -> Tuple[int, int, int]:
        raise UnsupportedCommandError(messages.imu.PoseRequest.msg_name())

    async def get_digital(self, port: int) -> bool:
        raise UnsupportedCommandError(messages.digital.Request.msg_name())

    async def set_motor(self, port: int, mode: int, amount: int=0,
                        reached_state: int=POWER, relative: int=None, absolute: int=None) -> None:
        # TODO separate set_motor and set_motor_positional command
        raise UnsupportedCommandError(messages.motor.Action.msg_name())

    # TODO add set_motor_servo command

    async def set_motor_config(self, port: int, config: motor.Config) -> None:
        raise UnsupportedCommandError(messages.motor.ConfigAction.msg_name())

    async def get_motor(self, port: int) -> Tuple[int, int]:
        raise UnsupportedCommandError(messages.motor.StateRequest.msg_name())

    async def set_motor_position(self, port: int, position: int) -> None:
        raise UnsupportedCommandError(messages.motor.SetPositionAction.msg_name())

    async def set_servo(self, port: int, active: bool, position: int) -> None:
        raise UnsupportedCommandError(messages.servo.Action.msg_name())

    async def send_uart(self, data: bytes) -> None:
        # TODO not exposed in protocol
        raise UnsupportedCommandError("send_uart")

    async def set_speaker(self, frequency: int) -> None:
        raise UnsupportedCommandError(messages.speaker.Action.msg_name())
