from . import HardwareAdapter, POWER

class SimulatedHardwareAdapter(HardwareAdapter):
    def get_analog(self, port):
        return 0

    def set_analog_state(self, port, pullup):
        # TODO set analog pullup
        pass

    def get_digital(self, port):
        return False

    def set_digital_state(self, port, pullup, output):
        # TODO set digital pullup, output
        pass

    def set_digital(self, port, level):
        # TODO set digital pullup, output
        pass

    def set_motor(self, port, state, amount=0, reached_state=POWER, relative=None, absolute=None):
        # TODO set motor action
        pass

    def get_motor(self, port):
        return 0, 0

    def set_motor_position(self, port, position):
        # TODO set motor position
        pass

    def set_servo(self, port, position):
        # TODO set servo position
        pass

    def set_servo_state(self, port, active):
        # TODO set servo active
        pass

