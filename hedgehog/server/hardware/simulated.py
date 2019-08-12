from typing import Dict

import random

from . import HardwareAdapter, POWER
from hedgehog.protocol.messages import io


class SimulatedHardwareAdapter(HardwareAdapter):
    def __init__(self, *args, simulate_sensors=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.simulate_sensors = simulate_sensors

        self.io_configs: Dict[int, int] = {}
        self.emergency: bool = False

    async def get_version(self):
        return bytes(12), 0, 0

    async def emergency_action(self, activate):
        self.emergency = activate

    async def get_emergency_state(self) -> bool:
        return self.emergency

    async def set_io_config(self, port, flags):
        self.io_configs[port] = flags

    async def get_analog(self, port):
        if not self.simulate_sensors:
            return 0

        mu, sigma = {
            io.INPUT_FLOATING: (800, 60),
            io.INPUT_PULLUP: (4030, 30),
            io.INPUT_PULLDOWN: (80, 30),
            io.OUTPUT_ON: (4050, 20),
            io.OUTPUT_OFF: (50, 20),
        }[self.io_configs.get(port, io.INPUT_FLOATING)]

        num = int(random.gauss(mu, sigma))
        if num < 0:
            num = 0
        if num >= 4096:
            num = 4095
        return num

    async def get_imu_rate(self):
        # TODO get_imu_rate
        return 0, 0, 0

    async def get_imu_acceleration(self):
        # TODO get_imu_acceleration
        return 0, 0, 0

    async def get_imu_pose(self):
        # TODO get_imu_pose
        return 0, 0, 0

    async def get_digital(self, port):
        if not self.simulate_sensors:
            return False

        value = {
            io.INPUT_FLOATING: False,
            io.INPUT_PULLUP: True,
            io.INPUT_PULLDOWN: False,
            io.OUTPUT_ON: True,
            io.OUTPUT_OFF: False,
        }[self.io_configs.get(port, io.INPUT_FLOATING)]
        return value

    async def set_motor(self, port, mode, amount=0, reached_state=POWER, relative=None, absolute=None):
        # TODO set motor action
        pass

    async def get_motor(self, port):
        return 0, 0

    async def set_motor_position(self, port, position):
        # TODO set motor position
        pass

    async def set_motor_config(self, port, config):
        # TODO set_motor_config
        pass

    async def set_servo(self, port, active, position):
        # TODO set_servo
        pass

    async def send_uart(self, data):
        # TODO send_uart
        pass

    async def set_speaker(self, frequency):
        # TODO set_speaker
        pass
