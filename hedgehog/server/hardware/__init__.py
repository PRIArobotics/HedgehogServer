from hedgehog.protocol.messages.motor import POWER, BRAKE, VELOCITY

class HardwareAdapter:
    def __init__(self, motor_state_update_cb=None):
        self.motor_state_update_cb = motor_state_update_cb

    def set_io_state(self, port, flags):
        raise NotImplementedError

    def get_analog(self, port):
        raise NotImplementedError

    def get_digital(self, port):
        raise NotImplementedError

    def set_motor(self, port, state, amount=0, reached_state=POWER, relative=None, absolute=None):
        raise NotImplementedError

    def get_motor(self, port):
        raise NotImplementedError

    def motor_state_update(self, port, state):
        if self.motor_state_update_cb is not None:
            self.motor_state_update_cb(port, state)

    def set_motor_position(self, port, position):
        raise NotImplementedError

    def set_servo(self, port, position):
        raise NotImplementedError

    def set_servo_state(self, port, active):
        raise NotImplementedError
