from typing import cast, Any, Dict, Tuple, Type

import itertools
import math
import time
from hedgehog.protocol import Header, Message
from hedgehog.protocol.errors import FailedCommandError, UnsupportedCommandError
from hedgehog.protocol.messages import ack, io, analog, digital, motor, servo
from hedgehog.protocol.proto.subscription_pb2 import Subscription
from hedgehog.utils.zmq.timer import TimerDefinition

from . import CommandHandler, command_handlers
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
        except KeyError as err:
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

    async def action(self, flags: int) -> None:
        await self.adapter.set_io_state(self.port, flags)
        await cast(subscription.TriggeredSubscribable[int, io.CommandUpdate],
                   self.subscribables[io.CommandSubscribe]).update(flags)
        self.command = flags,

    @property
    async def analog_value(self) -> int:
        return await self.adapter.get_analog(self.port)

    @property
    async def digital_value(self) -> bool:
        return await self.adapter.get_digital(self.port)


class _MotorHandler(_HWHandler):
    def __command_subscribable(self) -> subscription.TriggeredSubscribable[Tuple[int, int], motor.CommandUpdate]:
        outer_self = self

        class Subs(subscription.TriggeredSubscribable[Tuple[int, int], motor.CommandUpdate]):
            def compose_update(self, server, ident, subscription, command):
                state, amount = command
                return motor.CommandUpdate(outer_self.port, state, amount, subscription)

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
        self.command = None  # type: Tuple[int, int]

        self.subscribables[motor.CommandSubscribe] = self.__command_subscribable()
        try:
            # TODO
            # self.state
            pass
        except UnsupportedCommandError:
            pass
        else:
            self.subscribables[motor.StateSubscribe] = self.__state_subscribable()

    async def action(self, state: int, amount: int, reached_state: int, relative: int, absolute: int) -> None:
        await self.adapter.set_motor(self.port, state, amount, reached_state, relative, absolute)
        await cast(subscription.TriggeredSubscribable[Tuple[int, int], motor.CommandUpdate],
                   self.subscribables[motor.CommandSubscribe]).update((state, amount))
        self.command = state, amount

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

    async def action(self, active: bool, position: int) -> None:
        await self.adapter.set_servo(self.port, active, position if active else 0)
        await cast(subscription.TriggeredSubscribable[Tuple[int, int], servo.CommandUpdate],
                   self.subscribables[servo.CommandSubscribe]).update((active, position))
        self.command = active, position


class HardwareHandler(CommandHandler):
    _handlers, _command = command_handlers()

    def __init__(self, adapter: HardwareAdapter) -> None:
        super().__init__()
        self.adapter = adapter
        # TODO hard-coded number of ports
        self.ios = {port: _IOHandler(adapter, port) for port in itertools.chain(range(0, 16), (0x80, 0x90, 0x91))}
        self.motors = [_MotorHandler(adapter, port) for port in range(0, 4)]
        self.servos = [_ServoHandler(adapter, port) for port in range(0, 6)]
        # self.motor_cb = {}
        # self.adapter.motor_state_update_cb = self.motor_state_update

    @_command(io.Action)
    async def io_state_action(self, server, ident, msg):
        await self.ios[msg.port].action(msg.flags)
        return ack.Acknowledgement()

    @_command(io.CommandRequest)
    async def io_command_request(self, server, ident, msg):
        command = self.ios[msg.port].command
        try:
            flags, = command
        except TypeError:
            raise FailedCommandError("no command executed yet")
        else:
            return io.CommandReply(msg.port, flags)

    @_command(io.CommandSubscribe)
    async def io_command_subscribe(self, server, ident, msg):
        await self.ios[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_command(analog.Request)
    async def analog_request(self, server, ident, msg):
        value = await self.ios[msg.port].analog_value
        return analog.Reply(msg.port, value)

    @_command(analog.Subscribe)
    async def analog_command_subscribe(self, server, ident, msg):
        await self.ios[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_command(digital.Request)
    async def digital_request(self, server, ident, msg):
        value = await self.ios[msg.port].digital_value
        return digital.Reply(msg.port, value)

    @_command(digital.Subscribe)
    async def digital_command_subscribe(self, server, ident, msg):
        await self.ios[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_command(motor.Action)
    async def motor_action(self, server, ident, msg):
        # if msg.relative is not None or msg.absolute is not None:
        #     # this action will end with a state update
        #     def cb(port, state):
        #         server.send_async(ident, motor.StateUpdate(port, state))
        #     self.motor_cb[msg.port] = cb
        await self.motors[msg.port].action(msg.state, msg.amount, msg.reached_state, msg.relative, msg.absolute)
        return ack.Acknowledgement()

    @_command(motor.CommandRequest)
    async def motor_command_request(self, server, ident, msg):
        command = self.motors[msg.port].command
        try:
            state, amount = command
        except TypeError:
            raise FailedCommandError("no command executed yet")
        else:
            return motor.CommandReply(msg.port, state, amount)

    @_command(motor.CommandSubscribe)
    async def motor_command_subscribe(self, server, ident, msg):
        await self.motors[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_command(motor.StateRequest)
    async def motor_state_request(self, server, ident, msg):
        velocity, position = await self.motors[msg.port].state
        return motor.StateReply(msg.port, velocity, position)

    @_command(motor.StateSubscribe)
    async def motor_state_subscribe(self, server, ident, msg):
        await self.motors[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    # async def motor_state_update(self, port, state):
    #     if port in self.motor_cb:
    #         self.motor_cb[port](port, state)
    #         del self.motor_cb[port]

    @_command(motor.SetPositionAction)
    async def motor_set_position_action(self, server, ident, msg):
        await self.motors[msg.port].set_position(msg.position)
        return ack.Acknowledgement()

    @_command(servo.Action)
    async def servo_action(self, server, ident, msg):
        await self.servos[msg.port].action(msg.active, msg.position)
        return ack.Acknowledgement()

    @_command(servo.CommandRequest)
    async def servo_command_request(self, server, ident, msg):
        command = self.servos[msg.port].command
        try:
            active, position = command
        except TypeError:
            raise FailedCommandError("no command executed yet")
        else:
            return servo.CommandReply(msg.port, active, position)

    @_command(servo.CommandSubscribe)
    async def servo_command_subscribe(self, server, ident, msg):
        await self.servos[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()
