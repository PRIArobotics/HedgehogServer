from hedgehog.protocol.errors import UnsupportedCommandError
from hedgehog.protocol.messages.motor import POWER, BRAKE, VELOCITY

class HardwareAdapter:
    def __init__(self, motor_state_update_cb=None):
        self.motor_state_update_cb = motor_state_update_cb

    def set_io_state(self, port, flags):
        raise UnsupportedCommandError('io_state_action')

    def get_analog(self, port):
        raise UnsupportedCommandError('analog_request')

    def get_digital(self, port):
        raise UnsupportedCommandError('digital_request')

    def set_motor(self, port, state, amount=0, reached_state=POWER, relative=None, absolute=None):
        raise UnsupportedCommandError('motor_action')

    def get_motor(self, port):
        raise UnsupportedCommandError('motor_request')

    def motor_state_update(self, port, state):
        if self.motor_state_update_cb is not None:
            self.motor_state_update_cb(port, state)

    def set_motor_position(self, port, position):
        raise UnsupportedCommandError('motor_set_position_action')

    def set_servo(self, port, active, position):
        raise UnsupportedCommandError('servo_action')
