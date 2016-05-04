from hedgehog.protocol.messages.motor import POWER, VELOCITY, FREEZE

class HardwareAdapter:
    def get_analog(self, port):
        raise NotImplementedError

    def set_analog_state(self, port, pullup):
        raise NotImplementedError

    def get_digital(self, port):
        raise NotImplementedError

    def set_digital_state(self, port, pullup, output):
        raise NotImplementedError

    def set_digital(self, port, level):
        raise NotImplementedError

    def set_motor(self, port, state, amount=0, reached_state=POWER, relative=None, absolute=None):
        raise NotImplementedError

    def get_motor(self, port):
        raise NotImplementedError

    def set_motor_position(self, port, position):
        raise NotImplementedError

    def set_servo(self, port, position):
        raise NotImplementedError

    def set_servo_state(self, port, active):
        raise NotImplementedError

