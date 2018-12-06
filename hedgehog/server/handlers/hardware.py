from typing import cast, Dict, Tuple, Type

import itertools
from hedgehog.protocol import Header, Message
from hedgehog.protocol.errors import FailedCommandError, UnsupportedCommandError
from hedgehog.protocol.messages import ack, io, analog, digital, motor, servo
from hedgehog.protocol.proto.subscription_pb2 import Subscription

from . import CommandHandler, CommandRegistry
from .. import subscription
from ..hardware import HardwareAdapter
from ..hedgehog_server import HedgehogServer


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
        await self.adapter.set_io_state(self.port, flags)
        self.command = flags,
        await self.action_update()

    @property
    async def analog_value(self) -> int:
        return await self.adapter.get_analog(self.port)

    @property
    async def digital_value(self) -> bool:
        return await self.adapter.get_digital(self.port)


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
        await cast(subscription.TriggeredSubscribable[Tuple[int, int], motor.CommandUpdate],
                   self.subscribables[motor.CommandSubscribe]).update((self.config, state, amount))

    async def action(self, state: int, amount: int, reached_state: int, relative: int, absolute: int) -> None:
        await self.adapter.set_motor(self.port, state, amount, reached_state, relative, absolute)
        self.command = state, amount
        await self.action_update()

    async def config_action(self, config: motor.Config) -> None:
        # await self.adapter.set_motor(self.port, state, amount, reached_state, relative, absolute)
        self.config = config
        await self.action_update()

    async def set_position(self, position: int) -> None:
        await self.adapter.set_motor_position(self.port, position)

    @property
    async def state(self) -> Tuple[int, int]:
        return await self.adapter.get_motor(self.port)


class _ServoHandler(_HWHandler):
    def __command_subscribable(self) -> subscription.TriggeredSubscribable[Tuple[int, int], servo.CommandUpdate]:
        outer_self = self

        class Subs(subscription.TriggeredSubscribable[Tuple[int, int], servo.CommandUpdate]):
            def compose_update(self, server, ident, subscription, command):
                active, position = command
                return servo.CommandUpdate(outer_self.port, active, position, subscription)

        return Subs()

    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_ServoHandler, self).__init__(adapter)
        self.port = port
        self.command = None  # type: Tuple[bool, int]

        self.subscribables[servo.CommandSubscribe] = self.__command_subscribable()

    async def action_update(self) -> None:
        if self.command is None:
            return
        active, position = self.command
        await cast(subscription.TriggeredSubscribable[Tuple[int, int], servo.CommandUpdate],
                   self.subscribables[servo.CommandSubscribe]).update((active, position))

    async def action(self, active: bool, position: int) -> None:
        await self.adapter.set_servo(self.port, active, position if active else 0)
        self.command = active, position
        await self.action_update()


class HardwareHandler(CommandHandler):
    _commands = CommandRegistry()

    def __init__(self, adapter: HardwareAdapter) -> None:
        super().__init__()
        self.adapter = adapter
        # TODO hard-coded number of ports
        self.ios = {port: _IOHandler(adapter, port) for port in itertools.chain(range(0, 16), (0x80, 0x90, 0x91))}
        self.motors = [_MotorHandler(adapter, port) for port in range(0, 4)]
        self.servos = [_ServoHandler(adapter, port) for port in range(0, 6)]
        # self.motor_cb = {}
        # self.adapter.motor_state_update_cb = self.motor_state_update

    @_commands.register(io.Action)
    async def io_state_action(self, server, ident, msg):
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

    # async def motor_state_update(self, port, state):
    #     if port in self.motor_cb:
    #         self.motor_cb[port](port, state)
    #         del self.motor_cb[port]

    @_commands.register(motor.SetPositionAction)
    async def motor_set_position_action(self, server, ident, msg):
        await self.motors[msg.port].set_position(msg.position)
        return ack.Acknowledgement()

    @_commands.register(servo.Action)
    async def servo_action(self, server, ident, msg):
        await self.servos[msg.port].action(msg.active, msg.position)
        return ack.Acknowledgement()

    @_commands.register(servo.CommandRequest)
    async def servo_command_request(self, server, ident, msg):
        command = self.servos[msg.port].command
        try:
            active, position = command
        except TypeError:
            raise FailedCommandError("no command executed yet")
        else:
            return servo.CommandReply(msg.port, active, position)

    @_commands.register(servo.CommandSubscribe)
    async def servo_command_subscribe(self, server, ident, msg):
        await self.servos[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        await self.servos[msg.port].action_update()
        return ack.Acknowledgement()
