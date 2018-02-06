from typing import Any, Dict, Generic, List, Tuple, TypeVar

import asyncio
import bisect

from . import HardwareAdapter, POWER


T = TypeVar('T')


class MockedState(Generic[T]):
    def __init__(self) -> None:
        self._times = []  # type: List[float]
        self._values = []  # type: List[T]

    def set(self, time: float, value: T) -> None:
        i = bisect.bisect_left(self._times, time)
        if i < len(self._times) and self._times[i] == time:
            self._values[i] = value
        else:
            self._times.insert(i, time)
            self._values.insert(i, value)

    def get(self, time: float=None, default: T=None)-> T:
        if time is None:
            time = asyncio.get_event_loop().time()

        i = bisect.bisect_right(self._times, time)
        if i == 0:
            return default
        return self._values[i - 1]


class MockedHardwareAdapter(HardwareAdapter):
    def __init__(self, *args: Any, simulate_sensors: bool=False, **kwargs: Any) -> None:
        super(MockedHardwareAdapter, self).__init__(*args, **kwargs)
        self.simulate_sensors = simulate_sensors

        self.io_states = {}  # type: Dict[int, int]
        self._analogs = [MockedState() for port in range(16)]  # type: List[MockedState[int]]
        self._digitals = [MockedState() for port in range(16)]  # type: List[MockedState[bool]]
        self._motors = [MockedState() for port in range(4)]  # type: List[MockedState[Tuple[float, float]]]

    async def set_io_state(self, port, flags):
        self.io_states[port] = flags

    def set_analog(self, port: int, time: float, value: int) -> None:
        self._analogs[port].set(time, value)

    async def get_analog(self, port):
        return self._analogs[port].get(default=0)

    def set_digital(self, port: int, time: float, value: bool) -> None:
        self._digitals[port].set(time, value)

    async def get_digital(self, port):
        return self._digitals[port].get(default=False)

    async def set_motor(self, port, state, amount=0, reached_state=POWER, relative=None, absolute=None):
        # TODO set motor action
        pass

    def set_motor_state(self, port: int, time: float, velocity: int, position: int) -> None:
        self._motors[port].set(time, (velocity, position))

    async def get_motor(self, port):
        return self._motors[port].get(default=(0, 0))

    async def set_motor_position(self, port, position):
        # TODO set motor position
        pass

    async def set_servo(self, port, active, position):
        # TODO set servo position
        pass

