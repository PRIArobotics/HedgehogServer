from typing import Dict

import random

from . import HardwareAdapter, POWER
from hedgehog.protocol.messages import io


class SimulatedHardwareAdapter(HardwareAdapter):
    def __init__(self, *args, simulate_sensors=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.simulate_sensors = simulate_sensors

        self.io_states = {}  # type: Dict[int, int]

    async def set_io_state(self, port, flags):
        self.io_states[port] = flags

    async def get_analog(self, port):
        if not self.simulate_sensors:
            return 0

        mu, sigma = {
            io.INPUT_FLOATING: (800, 60),
            io.INPUT_PULLUP: (4030, 30),
            io.INPUT_PULLDOWN: (80, 30),
            io.OUTPUT_ON: (4050, 20),
            io.OUTPUT_OFF: (50, 20),
        }[self.io_states.get(port, io.INPUT_FLOATING)]

        num = int(random.gauss(mu, sigma))
        if num < 0:
            num = 0
        if num >= 4096:
            num = 4095
        return num

    async def get_digital(self, port):
        if not self.simulate_sensors:
            return False

        value = {
            io.INPUT_FLOATING: False,
            io.INPUT_PULLUP: True,
            io.INPUT_PULLDOWN: False,
            io.OUTPUT_ON: True,
            io.OUTPUT_OFF: False,
        }[self.io_states.get(port, io.INPUT_FLOATING)]
        return value

    async def set_motor(self, port, state, amount=0, reached_state=POWER, relative=None, absolute=None):
        # TODO set motor action
        pass

    async def get_motor(self, port):
        return 0, 0

    async def set_motor_position(self, port, position):
        # TODO set motor position
        pass

    async def set_servo(self, port, active, position):
        # TODO set servo position
        pass

