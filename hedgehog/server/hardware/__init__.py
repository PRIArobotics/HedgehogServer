from typing import Tuple

from hedgehog.protocol.errors import UnsupportedCommandError
from hedgehog.protocol import messages
from hedgehog.protocol.messages.motor import POWER


class HardwareAdapter(object):
    def __init__(self, motor_state_update_cb=None) -> None:
        self.motor_state_update_cb = motor_state_update_cb

    def set_io_state(self, port: int, flags: int) -> None:
        raise UnsupportedCommandError.from_msg_class(messages.io.StateAction)

    def get_analog(self, port: int) -> int:
        raise UnsupportedCommandError.from_msg_class(messages.analog.Request)

    def get_digital(self, port: int) -> bool:
        raise UnsupportedCommandError.from_msg_class(messages.digital.Request)

    def set_motor(self, port: int, state: int, amount: int=0,
                  reached_state: int=POWER, relative: int=None, absolute: int=None) -> None:
        raise UnsupportedCommandError.from_msg_class(messages.motor.Action)

    def get_motor(self, port: int) -> Tuple[int, int]:
        raise UnsupportedCommandError.from_msg_class(messages.motor.StateRequest)

    def motor_state_update(self, port: int, state: int) -> None:
        if self.motor_state_update_cb is not None:
            self.motor_state_update_cb(port, state)

    def set_motor_position(self, port: int, position: int) -> None:
        raise UnsupportedCommandError.from_msg_class(messages.motor.SetPositionAction)

    def set_servo(self, port: int, active: bool, position: int) -> None:
        raise UnsupportedCommandError.from_msg_class(messages.servo.Action)
