from typing import Any, Dict, Tuple, Type

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


class SubscriptionInfo(object):
    def __init__(self, server: HedgehogServer, ident: Header, subscription: Subscription) -> None:
        self.server = server
        self.ident = ident
        self.subscription = subscription
        self.counter = 0

        self._last_time = None  # type: float
        self._last_value = None  # type: Any

    @property
    def last_time(self) -> float:
        return self._last_time

    @property
    def last_value(self) -> Any:
        return self._last_value

    @last_value.setter
    def last_value(self, value: Any) -> None:
        # setting the value to none is to force an update at the next possible point, not to delay it further
        if value is not None:
            self._last_time = time.time()
        self._last_value = value

    async def send_async(self, *msgs: Message) -> None:
        await self.server.send_async(self.ident, *msgs)

    def handle_subscribe(self):
        self.counter += 1

    def handle_unsubscribe(self):
        self.counter -= 1


class CommandSubscriptionInfo(SubscriptionInfo):
    def __init__(self, *args, **kwargs) -> None:
        super(CommandSubscriptionInfo, self).__init__(*args, **kwargs)
        self.timer = None  # type: TimerDefinition

    def handle_subscribe(self) -> None:
        self.last_value = None
        self.schedule_update()

        super(CommandSubscriptionInfo, self).handle_subscribe()

    def handle_unsubscribe(self) -> None:
        super(CommandSubscriptionInfo, self).handle_unsubscribe()

        if self.counter == 0 and self.timer is not None:
            self.server.timer.unregister(self.timer)

    def schedule_update(self) -> None:
        command = self.command
        if command is None:
            return

        if self.last_value == command and self.timer is not None:
            # there is a timer that should be cancelled, because the value is no longer different
            self.server.timer.unregister(self.timer)
            self.timer = None
        elif self.last_value != command and self.timer is None:
            # add a timer for the update, either immediately, or at the earliest possible time
            now = time.time()
            earliest = now if self.last_time is None else self.last_time + self.subscription.timeout / 1000
            timeout = 0 if earliest <= now else earliest - now

            self.timer = self.server.timer.register(timeout, self.handle_update, repeat=False)

    def handle_update(self) -> None:
        self.timer = None

        command = self.command
        if command is None:
            return

        if self.last_value != command:
            self.last_value = command
            self.send_update(command)

    @property
    def command(self):
        raise NotImplementedError()

    def send_update(self, command):
        raise NotImplementedError()


class SensorSubscriptionInfo(SubscriptionInfo):
    def __init__(self, *args, **kwargs) -> None:
        super(SensorSubscriptionInfo, self).__init__(*args, **kwargs)
        self.timer = None  # type: TimerDefinition

    def handle_subscribe(self) -> None:
        self.last_value = None
        if self.counter == 0:
            # oversample 3 times, round delay up in milliseconds so we're not barely below the actual timeout
            timeout = math.ceil(self.subscription.timeout / 3)
            self.timer = self.server.timer.register(timeout / 1000, self.handle_update)

        super(SensorSubscriptionInfo, self).handle_subscribe()

    def handle_unsubscribe(self) -> None:
        super(SensorSubscriptionInfo, self).handle_unsubscribe()

        if self.counter == 0:
            self.server.timer.unregister(self.timer)

    def handle_update(self) -> None:
        value = self.value
        if value is None:
            return

        now = time.time()
        earliest = now if self.last_time is None else self.last_time + self.subscription.timeout / 1000

        if value != self.last_value and now >= earliest:
            self.last_value = value
            self.send_update(value)

    @property
    def value(self):
        raise NotImplementedError()

    def send_update(self, value):
        raise NotImplementedError()


class SubscriptionManager(object):
    def __init__(self, SubscriptionType: Type[SubscriptionInfo]) -> None:
        self.SubscriptionType = SubscriptionType
        self.subscriptions = {}  # type: Dict[Tuple[Header, int], SubscriptionInfo]

    def subscribe(self, server: HedgehogServer, ident: Header, subscription: Subscription) -> None:
        # TODO incomplete
        key = (ident, subscription.timeout)

        if subscription.subscribe:
            if key not in self.subscriptions:
                info = self.SubscriptionType(server, ident, subscription)
                self.subscriptions[key] = info
            else:
                info = self.subscriptions[key]
            info.handle_subscribe()
        else:
            try:
                info = self.subscriptions[key]
            except KeyError:
                raise FailedCommandError("can't cancel nonexistent subscription")
            else:
                info.handle_unsubscribe()
                if info.counter == 0:
                    del self.subscriptions[key]


class _HWHandler(object):
    def __init__(self, adapter: HardwareAdapter) -> None:
        self.adapter = adapter
        self.subscription_managers = {}  # type: Dict[Type[Message], SubscriptionManager]

    def subscribe(self, server: HedgehogServer, ident: Header, msg: Type[Message], subscription: Subscription) -> None:
        try:
            mgr = self.subscription_managers[msg]
        except KeyError as err:
            raise UnsupportedCommandError(msg.msg_name()) from err
        else:
            mgr.subscribe(server, ident, subscription)


class _IOHandler(_HWHandler):
    def __command_subscribable(self) -> subscription.TriggeredSubscribable:
        outer_self = self

        class Subs(subscription.TriggeredSubscribable):
            def compose_update(self, server: HedgehogServer, ident: Header, subscription: Subscription, flags: int):
                return io.CommandUpdate(outer_self.port, flags, subscription)

        return Subs()

    def __analog_subscribable(self) -> subscription.PolledSubscribable:
        outer_self = self

        class Subs(subscription.PolledSubscribable):
            async def poll(self):
                return await outer_self.analog_value

            def compose_update(self, server: HedgehogServer, ident: Header, subscription: Subscription, value: int):
                return analog.Update(outer_self.port, value, subscription)

        return Subs()

    def __digital_subscribable(self) -> subscription.PolledSubscribable:
        outer_self = self

        class Subs(subscription.PolledSubscribable):
            async def poll(self):
                return await outer_self.digital_value

            def compose_update(self, server: HedgehogServer, ident: Header, subscription: Subscription, value: bool):
                return digital.Update(outer_self.port, value, subscription)

        return Subs()

    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_IOHandler, self).__init__(adapter)
        self.port = port
        self.command = None  # type: Tuple[int]
        self._command = self.__command_subscribable()
        self._analog_value = self.__analog_subscribable()
        self._digital_value = self.__digital_subscribable()

    async def action(self, flags: int) -> None:
        await self.adapter.set_io_state(self.port, flags)
        await self._command.update(flags)
        self.command = flags,

    @property
    async def analog_value(self) -> int:
        return await self.adapter.get_analog(self.port)

    @property
    async def digital_value(self) -> bool:
        return await self.adapter.get_digital(self.port)


class _MotorHandler(_HWHandler):
    def __command_subscription_manager(self) -> SubscriptionManager:
        outer_self = self

        class Info(CommandSubscriptionInfo):
            @property
            def command(self):
                return outer_self.command

            def send_update(self, command):
                state, amount = command
                self.send_async(motor.CommandUpdate(outer_self.port, state, amount, self.subscription))

        return SubscriptionManager(Info)

    def __state_subscription_manager(self) -> SubscriptionManager:
        outer_self = self

        class Info(SensorSubscriptionInfo):
            @property
            def value(self):
                return outer_self.state

            def send_update(self, value):
                velocity, position = value
                self.send_async(motor.StateUpdate(outer_self.port, velocity, position, self.subscription))

        return SubscriptionManager(Info)

    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_MotorHandler, self).__init__(adapter)
        self.port = port
        self.command = None  # type: Tuple[int, int]

        self.subscription_managers[motor.CommandSubscribe] = self.__command_subscription_manager()
        try:
            # TODO
            # self.state
            pass
        except UnsupportedCommandError:
            pass
        else:
            self.subscription_managers[motor.StateSubscribe] = self.__state_subscription_manager()

    async def action(self, state: int, amount: int, reached_state: int, relative: int, absolute: int) -> None:
        await self.adapter.set_motor(self.port, state, amount, reached_state, relative, absolute)
        self.command = state, amount

    async def set_position(self, position: int) -> None:
        await self.adapter.set_motor_position(self.port, position)

    @property
    async def state(self) -> Tuple[int, int]:
        return await self.adapter.get_motor(self.port)


class _ServoHandler(_HWHandler):
    def __command_subscription_manager(self) -> SubscriptionManager:
        outer_self = self

        class Info(CommandSubscriptionInfo):
            @property
            def command(self):
                return outer_self.command

            def send_update(self, command):
                active, position = command
                self.send_async(servo.CommandUpdate(outer_self.port, active, position, self.subscription))

        return SubscriptionManager(Info)

    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_ServoHandler, self).__init__(adapter)
        self.port = port
        self.command = None  # type: Tuple[bool, int]

        self.subscription_managers[servo.CommandSubscribe] = self.__command_subscription_manager()

    async def action(self, active: bool, position: int) -> None:
        await self.adapter.set_servo(self.port, active, position if active else 0)
        self.command = active, position


class HardwareHandler(CommandHandler):
    _handlers, _command = command_handlers()

    def __init__(self, adapter):
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
        # self.ios[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        await self.ios[msg.port]._command.subscribe(server, ident, msg.subscription)
        return ack.Acknowledgement()

    @_command(analog.Request)
    async def analog_request(self, server, ident, msg):
        value = await self.ios[msg.port].analog_value
        return analog.Reply(msg.port, value)

    @_command(analog.Subscribe)
    async def analog_command_subscribe(self, server, ident, msg):
        await self.ios[msg.port]._analog_value.subscribe(server, ident, msg.subscription)
        return ack.Acknowledgement()

    @_command(digital.Request)
    async def digital_request(self, server, ident, msg):
        value = await self.ios[msg.port].digital_value
        return digital.Reply(msg.port, value)

    @_command(digital.Subscribe)
    async def digital_command_subscribe(self, server, ident, msg):
        await self.ios[msg.port]._digital_value.subscribe(server, ident, msg.subscription)
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
        self.motors[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_command(motor.StateRequest)
    async def motor_state_request(self, server, ident, msg):
        velocity, position = await self.motors[msg.port].state
        return motor.StateReply(msg.port, velocity, position)

    @_command(motor.StateSubscribe)
    async def motor_state_subscribe(self, server, ident, msg):
        self.motors[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
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
        self.servos[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()
