from typing import Tuple

from hedgehog.protocol.errors import UnsupportedCommandError
from hedgehog.protocol import messages
from hedgehog.protocol.messages import io, analog, digital, servo
from hedgehog.protocol.messages.motor import POWER


class HardwareAdapter(object):
    def __init__(self, motor_state_update_cb=None) -> None:
        self.motor_state_update_cb = motor_state_update_cb

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

    async def motor_state_update(self, port: int, state: int) -> None:
        if self.motor_state_update_cb is not None:
            self.motor_state_update_cb(port, state)

    async def set_motor_position(self, port: int, position: int) -> None:
        raise UnsupportedCommandError(messages.motor.SetPositionAction.msg_name())

    async def set_servo(self, port: int, active: bool, position: int) -> None:
        raise UnsupportedCommandError(messages.servo.Action.msg_name())
