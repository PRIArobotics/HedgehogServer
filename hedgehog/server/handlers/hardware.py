from typing import cast, Dict, List, Optional, Tuple, Type

from contextlib import AsyncExitStack
import itertools
import logging
from hedgehog.protocol import Header, Message
from hedgehog.protocol.errors import HedgehogCommandError, FailedCommandError, UnsupportedCommandError
from hedgehog.protocol.messages import ack, version, emergency, io, analog, digital, imu, motor, servo, speaker
from hedgehog.protocol.proto.subscription_pb2 import Subscription

from . import CommandHandler, CommandRegistry
from .. import subscription
from ..hardware import HardwareAdapter
from ..hedgehog_server import HedgehogServer

logger = logging.getLogger(__name__)


class _HWHandler(object):
    def __init__(self, adapter: HardwareAdapter) -> None:
        self.adapter = adapter
        self.subscribables = {}  # type: Dict[Type[Message], subscription.Subscribable]

    async def subscribe(self, server: HedgehogServer, ident: Header, msg: Type[Message], subscription: Subscription) -> None:
        try:
            subscribable = self.subscribables[msg]
        except KeyError as err:  # pragma: nocover
            raise UnsupportedCommandError(msg.msg_name()) from err
        else:
            await subscribable.subscribe(server, ident, subscription)


class _EmergencyHandler(_HWHandler):
    def __state_subscribable(self) -> subscription.TriggeredSubscribable[bool, emergency.Update]:
        outer_self = self

        class Subs(subscription.TriggeredSubscribable[bool, emergency.Update]):
            def compose_update(self, server, ident, subscription, active):
                return emergency.Update(active, subscription)

        return Subs()

    def __init__(self, adapter: HardwareAdapter) -> None:
        super(_EmergencyHandler, self).__init__(adapter)
        self.state = None  # type: Tuple[bool]
        self.subscribables[emergency.Subscribe] = self.__state_subscribable()

    async def state_update(self) -> None:
        if self.state is None:
            return
        active, = self.state
        await cast(subscription.TriggeredSubscribable[bool, emergency.Update],
                   self.subscribables[emergency.Subscribe]).update(active)

    async def action(self, activate: bool) -> None:
        await self.adapter.emergency_action(activate)
        self.state = activate,
        await self.state_update()

    @property
    async def emergency_state(self) -> bool:
        # don't update the saved state; do that when the HWC sends an update by itself
        return await self.adapter.get_emergency_state()


class _IOHandler(_HWHandler):
    def __command_subscribable(self) -> subscription.TriggeredSubscribable[int, io.CommandUpdate]:
        outer_self = self

        class Subs(subscription.TriggeredSubscribable[int, io.CommandUpdate]):
            def compose_update(self, server, ident, subscription, flags):
                return io.CommandUpdate(outer_self.port, flags, subscription)

        return Subs()

    def __analog_subscribable(self) -> subscription.PolledSubscribable[int, analog.Update]:
        outer_self = self

        class Subs(subscription.PolledSubscribable[int, analog.Update]):
            async def poll(self):
                return await outer_self.analog_value

            def compose_update(self, server, ident, subscription, value):
                return analog.Update(outer_self.port, value, subscription)

        return Subs()

    def __digital_subscribable(self) -> subscription.PolledSubscribable[bool, digital.Update]:
        outer_self = self

        class Subs(subscription.PolledSubscribable[bool, digital.Update]):
            async def poll(self):
                return await outer_self.digital_value

            def compose_update(self, server, ident, subscription, value):
                return digital.Update(outer_self.port, value, subscription)

        return Subs()

    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_IOHandler, self).__init__(adapter)
        self.port = port
        self.command = None  # type: Tuple[int]
        self.subscribables[io.CommandSubscribe] = self.__command_subscribable()
        self.subscribables[analog.Subscribe] = self.__analog_subscribable()
        self.subscribables[digital.Subscribe] = self.__digital_subscribable()

    async def action_update(self) -> None:
        if self.command is None:
            return
        flags, = self.command
        await cast(subscription.TriggeredSubscribable[int, io.CommandUpdate],
                   self.subscribables[io.CommandSubscribe]).update(flags)

    async def action(self, flags: int) -> None:
        await self.adapter.set_io_config(self.port, flags)
        self.command = flags,
        await self.action_update()

    @property
    async def analog_value(self) -> int:
        return await self.adapter.get_analog(self.port)

    @property
    async def digital_value(self) -> bool:
        return await self.adapter.get_digital(self.port)


class _IMUHandler(_HWHandler):
    def __rate_subscribable(self) -> subscription.PolledSubscribable[Tuple[int, int, int], imu.RateUpdate]:
        outer_self = self

        class Subs(subscription.PolledSubscribable[Tuple[int, int, int], imu.RateUpdate]):
            async def poll(self):
                return await outer_self.rate_value

            def compose_update(self, server, ident, subscription, value):
                return imu.RateUpdate(*value, subscription)

        return Subs()

    def __accelleration_subscribable(self) -> subscription.PolledSubscribable[Tuple[int, int, int], imu.AccelerationUpdate]:
        outer_self = self

        class Subs(subscription.PolledSubscribable[Tuple[int, int, int], imu.AccelerationUpdate]):
            async def poll(self):
                return await outer_self.acceleration_value

            def compose_update(self, server, ident, subscription, value):
                return imu.AccelerationUpdate(*value, subscription)

        return Subs()

    def __pose_subscribable(self) -> subscription.PolledSubscribable[Tuple[int, int, int], imu.PoseUpdate]:
        outer_self = self

        class Subs(subscription.PolledSubscribable[Tuple[int, int, int], imu.PoseUpdate]):
            async def poll(self):
                return await outer_self.pose_value

            def compose_update(self, server, ident, subscription, value):
                return imu.PoseUpdate(*value, subscription)

        return Subs()

    def __init__(self, adapter: HardwareAdapter) -> None:
        super(_IMUHandler, self).__init__(adapter)
        self.subscribables[imu.RateSubscribe] = self.__rate_subscribable()
        self.subscribables[imu.AccelerationSubscribe] = self.__accelleration_subscribable()
        self.subscribables[imu.PoseSubscribe] = self.__pose_subscribable()

    @property
    async def rate_value(self) -> Tuple[int, int, int]:
        return await self.adapter.get_imu_rate()

    @property
    async def acceleration_value(self) -> Tuple[int, int, int]:
        return await self.adapter.get_imu_acceleration()

    @property
    async def pose_value(self) -> Tuple[int, int, int]:
        return await self.adapter.get_imu_pose()


class _MotorHandler(_HWHandler):
    def __command_subscribable(self) -> subscription.TriggeredSubscribable[Tuple[motor.Config, int, int], motor.CommandUpdate]:
        outer_self = self

        class Subs(subscription.TriggeredSubscribable[Tuple[motor.Config, int, int], motor.CommandUpdate]):
            def compose_update(self, server, ident, subscription, command):
                config, state, amount = command
                return motor.CommandUpdate(outer_self.port, config, state, amount, subscription)

        return Subs()

    def __state_subscribable(self) -> subscription.PolledSubscribable[Tuple[int, int], motor.StateUpdate]:
        outer_self = self

        class Subs(subscription.PolledSubscribable[Tuple[int, int], motor.StateUpdate]):
            async def poll(self):
                return await outer_self.state

            def compose_update(self, server, ident, subscription, state):
                velocity, position = state
                return motor.StateUpdate(outer_self.port, velocity, position, subscription)

        return Subs()

    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_MotorHandler, self).__init__(adapter)
        self.port = port
        self.config = motor.DcConfig()  # type: motor.Config
        self.command = None  # type: Tuple[int, int]

        self.subscribables[motor.CommandSubscribe] = self.__command_subscribable()
        try:
            # TODO
            # self.state
            pass
        except UnsupportedCommandError:  # pragma: nocover
            pass
        else:
            self.subscribables[motor.StateSubscribe] = self.__state_subscribable()

    async def action_update(self) -> None:
        if self.command is None:
            return
        state, amount = self.command
        await cast(subscription.TriggeredSubscribable[Tuple[motor.Config, int, int], motor.CommandUpdate],
                   self.subscribables[motor.CommandSubscribe]).update((self.config, state, amount))

    async def action(self, state: int, amount: int, reached_state: int, relative: int, absolute: int) -> None:
        await self.adapter.set_motor(self.port, state, amount, reached_state, relative, absolute)
        self.command = state, amount
        await self.action_update()

    async def config_action(self, config: motor.Config) -> None:
        await self.adapter.set_motor_config(self.port, config)
        self.config = config
        await self.action_update()

    async def set_position(self, position: int) -> None:
        await self.adapter.set_motor_position(self.port, position)

    @property
    async def state(self) -> Tuple[int, int]:
        return await self.adapter.get_motor(self.port)


class _ServoHandler(_HWHandler):
    def __command_subscribable(self) -> subscription.TriggeredSubscribable[Optional[int], servo.CommandUpdate]:
        outer_self = self

        class Subs(subscription.TriggeredSubscribable[Optional[int], servo.CommandUpdate]):
            def compose_update(self, server, ident, subscription, position):
                return servo.CommandUpdate(outer_self.port, position, subscription)

        return Subs()

    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_ServoHandler, self).__init__(adapter)
        self.port = port
        self.command = None  # type: Tuple[Optional[int]]

        self.subscribables[servo.CommandSubscribe] = self.__command_subscribable()

    async def action_update(self) -> None:
        if self.command is None:
            return
        position, = self.command
        await cast(subscription.TriggeredSubscribable[Optional[int], servo.CommandUpdate],
                   self.subscribables[servo.CommandSubscribe]).update(position)

    async def action(self, position: Optional[int]) -> None:
        await self.adapter.set_servo(self.port, position is not None, position if position is not None else 0)
        self.command = position,
        await self.action_update()


class HardwareHandler(CommandHandler):
    _commands = CommandRegistry()

    def __init__(self, adapter: HardwareAdapter) -> None:
        super().__init__()
        self.adapter = adapter
        self._stack: AsyncExitStack = None
        self.emergency: _EmergencyHandler = None
        self.imu: _IMUHandler = None
        self.ios: Dict[int, _IOHandler] = None
        self.motors: List[_MotorHandler] = None
        self.servos: List[_ServoHandler] = None
        # self.motor_cb = {}
        # self.adapter.motor_state_update_cb = self.motor_state_update

    async def __aenter__(self):
        self._stack = AsyncExitStack()
        await self._stack.enter_async_context(self.adapter)

        try:
            uc_id, hw_version, sw_version = await self.adapter.get_version()

            if uc_id == bytes(12):
                logger.debug(f"HWC ID = {':'.join(f'{b:02x}' for b in uc_id)} (simulated)")
            else:  # pragma: nocover
                logger.debug(f"HWC ID = {':'.join(f'{b:02x}' for b in uc_id)}")
            logger.debug(f"HWC hardware version = {hw_version}")
            logger.debug(f"HWC firmware version = {sw_version}")
        except HedgehogCommandError:
            logger.debug(f"HWC firmware old (get_version failed)")

            uc_id, hw_version, sw_version = bytes(12), 3, 0

        port_numbers = {
            # hw_version: IO, motors, servos
            # TODO HW revisions 0-2
            3: (16, 4, 6),
        }

        # default to HW version 3 (
        effective_hw_version = hw_version if hw_version in port_numbers else 3

        ios, motors, servos = port_numbers[effective_hw_version]

        self.emergency = _EmergencyHandler(self.adapter)
        self.imu = _IMUHandler(self.adapter)
        self.ios = {port: _IOHandler(self.adapter, port) for port in itertools.chain(range(0, ios), (0x80, 0x90, 0x91))}
        self.motors = [_MotorHandler(self.adapter, port) for port in range(0, motors)]
        self.servos = [_ServoHandler(self.adapter, port) for port in range(0, servos)]

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._stack.__aexit__(exc_type, exc_val, exc_tb)

    @_commands.register(version.Request)
    async def version_request(self, server, ident, msg):
        # avoid cyclic import
        from .. import __version__ as server_version

        uc_id, hw_version, sw_version = await self.adapter.get_version()
        return version.Reply(uc_id, str(hw_version), str(sw_version), server_version)

    @_commands.register(emergency.Action)
    async def emergency_release_action(self, server, ident, msg):
        await self.emergency.action(msg.activate)
        return ack.Acknowledgement()

    @_commands.register(emergency.Request)
    async def emergency_request(self, server, ident, msg):
        active = await self.emergency.emergency_state
        return emergency.Reply(active)

    @_commands.register(emergency.Subscribe)
    async def emergency_subscribe(self, server, ident, msg):
        await self.emergency.subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_commands.register(io.Action)
    async def io_config_action(self, server, ident, msg):
        await self.ios[msg.port].action(msg.flags)
        return ack.Acknowledgement()

    @_commands.register(io.CommandRequest)
    async def io_command_request(self, server, ident, msg):
        command = self.ios[msg.port].command
        try:
            flags, = command
        except TypeError:
            raise FailedCommandError("no command executed yet")
        else:
            return io.CommandReply(msg.port, flags)

    @_commands.register(io.CommandSubscribe)
    async def io_command_subscribe(self, server, ident, msg):
        await self.ios[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        await self.ios[msg.port].action_update()
        return ack.Acknowledgement()

    @_commands.register(analog.Request)
    async def analog_request(self, server, ident, msg):
        value = await self.ios[msg.port].analog_value
        return analog.Reply(msg.port, value)

    @_commands.register(analog.Subscribe)
    async def analog_subscribe(self, server, ident, msg):
        await self.ios[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_commands.register(digital.Request)
    async def digital_request(self, server, ident, msg):
        value = await self.ios[msg.port].digital_value
        return digital.Reply(msg.port, value)

    @_commands.register(digital.Subscribe)
    async def digital_subscribe(self, server, ident, msg):
        await self.ios[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_commands.register(imu.RateRequest)
    async def imu_rate_request(self, server, ident, msg):
        value = await self.imu.rate_value
        return imu.RateReply(*value)

    @_commands.register(imu.RateSubscribe)
    async def imu_rate_subscribe(self, server, ident, msg):
        await self.imu.subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_commands.register(imu.AccelerationRequest)
    async def imu_acceleration_request(self, server, ident, msg):
        value = await self.imu.acceleration_value
        return imu.AccelerationReply(*value)

    @_commands.register(imu.AccelerationSubscribe)
    async def imu_acceleration_subscribe(self, server, ident, msg):
        await self.imu.subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_commands.register(imu.PoseRequest)
    async def imu_pose_request(self, server, ident, msg):
        value = await self.imu.pose_value
        return imu.PoseReply(*value)

    @_commands.register(imu.PoseSubscribe)
    async def imu_pose_subscribe(self, server, ident, msg):
        await self.imu.subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_commands.register(motor.Action)
    async def motor_action(self, server, ident, msg):
        # if msg.relative is not None or msg.absolute is not None:
        #     # this action will end with a state update
        #     def cb(port, state):
        #         server.send_async(ident, motor.StateUpdate(port, state))
        #     self.motor_cb[msg.port] = cb
        await self.motors[msg.port].action(msg.state, msg.amount, msg.reached_state, msg.relative, msg.absolute)
        return ack.Acknowledgement()

    @_commands.register(motor.ConfigAction)
    async def motor_config_action(self, server, ident, msg):
        await self.motors[msg.port].config_action(msg.config)
        return ack.Acknowledgement()

    @_commands.register(motor.CommandRequest)
    async def motor_command_request(self, server, ident, msg):
        config = self.motors[msg.port].config
        command = self.motors[msg.port].command
        try:
            state, amount = command
        except TypeError:
            raise FailedCommandError("no command executed yet")
        else:
            return motor.CommandReply(msg.port, config, state, amount)

    @_commands.register(motor.CommandSubscribe)
    async def motor_command_subscribe(self, server, ident, msg):
        await self.motors[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        await self.motors[msg.port].action_update()
        return ack.Acknowledgement()

    @_commands.register(motor.StateRequest)
    async def motor_state_request(self, server, ident, msg):
        velocity, position = await self.motors[msg.port].state
        return motor.StateReply(msg.port, velocity, position)

    @_commands.register(motor.StateSubscribe)
    async def motor_state_subscribe(self, server, ident, msg):
        await self.motors[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_commands.register(motor.SetPositionAction)
    async def motor_set_position_action(self, server, ident, msg):
        await self.motors[msg.port].set_position(msg.position)
        return ack.Acknowledgement()

    @_commands.register(servo.Action)
    async def servo_action(self, server, ident, msg):
        await self.servos[msg.port].action(msg.position)
        return ack.Acknowledgement()

    @_commands.register(servo.CommandRequest)
    async def servo_command_request(self, server, ident, msg):
        command = self.servos[msg.port].command
        try:
            position, = command
        except TypeError:
            raise FailedCommandError("no command executed yet")
        else:
            return servo.CommandReply(msg.port, position)

    @_commands.register(servo.CommandSubscribe)
    async def servo_command_subscribe(self, server, ident, msg):
        await self.servos[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        await self.servos[msg.port].action_update()
        return ack.Acknowledgement()

    @_commands.register(speaker.Action)
    async def speaker_action(self, server, ident, msg):
        await self.adapter.set_speaker(msg.frequency)
        return ack.Acknowledgement()
