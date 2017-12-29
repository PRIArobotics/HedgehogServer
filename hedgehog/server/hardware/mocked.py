from typing import Dict

import asyncio
import bisect

from . import HardwareAdapter, POWER


class MockedState(object):
    def __init__(self):
        self._times = []
        self._values = []

    def set(self, time, value):
        i = bisect.bisect_left(self._times, time)
        if i < len(self._times) and self._times[i] == time:
            self._values[i] = value
        else:
            self._times.insert(i, time)
            self._values.insert(i, value)

    def get(self, time=None, default=None):
        if time is None:
            time = asyncio.get_event_loop().time()

        i = bisect.bisect_right(self._times, time)
        if i == 0:
            return default
        return self._values[i - 1]


class MockedHardwareAdapter(HardwareAdapter):
    def __init__(self, *args, simulate_sensors=False, **kwargs):
        super(MockedHardwareAdapter, self).__init__(*args, **kwargs)
        self.simulate_sensors = simulate_sensors

        self.io_states = {}  # type: Dict[int, int]
        self._analogs = [MockedState() for port in range(16)]
        self._digitals = [MockedState() for port in range(16)]
        self._motors = [MockedState() for port in range(4)]

    async def set_io_state(self, port, flags):
        self.io_states[port] = flags

    def set_analog(self, port, time, value):
        self._analogs[port].set(time, value)

    async def get_analog(self, port):
        return self._analogs[port].get(default=0)

    def set_digital(self, port, time, value):
        self._digitals[port].set(time, value)

    async def get_digital(self, port):
        return self._analogs[port].get(default=False)

    async def set_motor(self, port, state, amount=0, reached_state=POWER, relative=None, absolute=None):
        # TODO set motor action
        pass

    def set_motor_state(self, port, time, velocity, position):
        self._analogs[port].set(time, (velocity, position))

    async def get_motor(self, port):
        return self._motors[port].get(default=(0, 0))

    async def set_motor_position(self, port, position):
        # TODO set motor position
        pass

    async def set_servo(self, port, active, position):
        # TODO set servo position
        pass

