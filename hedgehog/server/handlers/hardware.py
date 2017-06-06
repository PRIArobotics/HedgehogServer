from typing import Any, Callable, Dict, Tuple, Type

import time
from hedgehog.protocol import Header, Message
from hedgehog.protocol.errors import UnsupportedCommandError, FailedCommandError
from hedgehog.protocol.messages import ack, io, analog, digital, motor, servo
from hedgehog.protocol.proto.subscription_pb2 import Subscription
from hedgehog.utils.zmq.timer import TimerDefinition

from . import CommandHandler, command_handlers
from ..hardware import HardwareAdapter
from ..hedgehog_server import HedgehogServerActor


class SubscriptionInfo(object):
    def __init__(self, server: HedgehogServerActor, ident: Header, subscription: Subscription) -> None:
        self.server = server
        self.ident = ident
        self.subscription = subscription
        self.counter = 0

        self._last_time = None  # type: float
        self._last_value = None  # type: Any

        self.extra = None  # type: Any

    @property
    def last_time(self) -> float:
        return self._last_time

    @property
    def last_value(self) -> Any:
        return self._last_value

    @last_value.setter
    def last_value(self, value: Any) -> None:
        self._last_time = time.time()
        self._last_value = value

    def should_send(self, value: Any) -> bool:
        if value == self.last_value:
            return False
        if self.subscription.timeout is not None and self.last_time is not None and time.time() < self.last_time + self.subscription.timeout / 1000:
            return False
        return True

    def send_async(self, *msgs: Message) -> None:
        self.server.send_async(self.ident, *msgs)


class SubscriptionManager(object):
    def __init__(self) -> None:
        self.subscriptions = {}  # type: Dict[Tuple[Header, int], SubscriptionInfo]

    def subscribe(self, server: HedgehogServerActor, ident: Header, subscription: Subscription) -> None:
        # TODO incomplete
        key = (ident, subscription.timeout)

        if subscription.subscribe:
            if key not in self.subscriptions:
                info = SubscriptionInfo(server, ident, subscription)
                self.subscriptions[key] = info
                self.handle_subscribe(info)
            else:
                info = self.subscriptions[key]
            info.counter += 1
        else:
            try:
                info = self.subscriptions[key]
            except KeyError:
                raise FailedCommandError("can't cancel nonexistent subscription")
            else:
                info.counter -= 1
                if info.counter == 0:
                    self.handle_unsubscribe(info)
                    del self.subscriptions[key]

    def handle_subscribe(self, info: SubscriptionInfo) -> None:
        pass

    def handle_unsubscribe(self, info: SubscriptionInfo) -> None:
        pass


class _HWHandler(object):
    def __init__(self, adapter: HardwareAdapter) -> None:
        self.adapter = adapter
        self.subscription_handlers = {}  # type: Dict[Type[Message], SubscriptionManager]

    def subscribe(self, server: HedgehogServerActor, ident: Header, msg: Type[Message], subscription: Subscription) -> None:
        self.subscription_handlers[msg].subscribe(server, ident, subscription)


class _IOHandler(_HWHandler):
    def __command_subscription_manager(self) -> SubscriptionManager:
        outer_self = self

        class Extra(object):
            timer = None  # type: TimerDefinition

        class Mgr(SubscriptionManager):
            def __init__(self) -> None:
                super(Mgr, self).__init__()

            def handle_subscribe(self, info: SubscriptionInfo) -> None:
                info.extra = Extra()
                info.extra.timer = info.server.timer.register(info.subscription.timeout / 1000, lambda: self.handle_update(info))

            def handle_unsubscribe(self, info: SubscriptionInfo) -> None:
                info.server.timer.unregister(info.extra.timer)

            def handle_update(self, info: SubscriptionInfo) -> None:
                try:
                    flags, = outer_self.command
                except TypeError:
                    pass
                else:
                    if info.should_send(flags):
                        info.last_value = flags
                        info.send_async(io.CommandUpdate(outer_self.port, flags, info.subscription))

        return Mgr()

    def __analog_subscription_manager(self) -> SubscriptionManager:
        outer_self = self

        class Extra(object):
            timer = None  # type: TimerDefinition

        class Mgr(SubscriptionManager):
            def __init__(self) -> None:
                super(Mgr, self).__init__()

            def handle_subscribe(self, info: SubscriptionInfo) -> None:
                info.extra = Extra()
                info.extra.timer = info.server.timer.register(info.subscription.timeout / 1000, lambda: self.handle_update(info))

            def handle_unsubscribe(self, info: SubscriptionInfo) -> None:
                info.server.timer.unregister(info.extra.timer)

            def handle_update(self, info: SubscriptionInfo) -> None:
                value = outer_self.analog_value
                if info.should_send(value):
                    info.last_value = value
                    info.send_async(analog.Update(outer_self.port, value, info.subscription))

        return Mgr()

    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_IOHandler, self).__init__(adapter)
        self.port = port
        self.command = None  # type: Tuple[int]

        self.subscription_handlers[io.CommandSubscribe] = self.__command_subscription_manager()
        self.subscription_handlers[analog.Subscribe] = self.__analog_subscription_manager()

    def action(self, flags: int) -> None:
        self.adapter.set_io_state(self.port, flags)
        self.command = flags,

    @property
    def analog_value(self) -> int:
        return self.adapter.get_analog(self.port)

    @property
    def digital_value(self) -> bool:
        return self.adapter.get_digital(self.port)


class _MotorHandler(_HWHandler):
    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_MotorHandler, self).__init__(adapter)
        self.port = port
        self.command = None  # type: Tuple[int, int]

    def action(self, state: int, amount: int, reached_state: int, relative: int, absolute: int) -> None:
        self.adapter.set_motor(self.port, state, amount, reached_state, relative, absolute)
        self.command = state, amount

    def set_position(self, position: int) -> None:
        self.adapter.set_motor_position(self.port, position)

    @property
    def state(self) -> Tuple[int, int]:
        return self.adapter.get_motor(self.port)


class _ServoHandler(_HWHandler):
    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_ServoHandler, self).__init__(adapter)
        self.port = port
        self.command = None  # type: Tuple[bool, int]

    def action(self, active: bool, position: int) -> None:
        self.adapter.set_servo(self.port, active, position)
        self.command = active, position


class HardwareHandler(CommandHandler):
    _handlers, _command = command_handlers()

    def __init__(self, adapter):
        super().__init__()
        self.adapter = adapter
        # TODO hard-coded number of ports
        self.ios = [_IOHandler(adapter, port) for port in range(0, 16)]
        self.motors = [_MotorHandler(adapter, port) for port in range(0, 4)]
        self.servos = [_ServoHandler(adapter, port) for port in range(0, 4)]
        # self.motor_cb = {}
        # self.adapter.motor_state_update_cb = self.motor_state_update

    @_command(io.Action)
    def analog_state_action(self, server, ident, msg):
        self.ios[msg.port].action(msg.flags)
        return ack.Acknowledgement()

    @_command(io.CommandRequest)
    def io_command_request(self, server, ident, msg):
        command = self.ios[msg.port].command
        try:
            flags, = command
        except TypeError:
            raise FailedCommandError("no command executed yet")
        else:
            return io.CommandReply(msg.port, flags)

    @_command(io.CommandSubscribe)
    def io_command_subscribe(self, server, ident, msg):
        self.ios[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_command(analog.Request)
    def analog_request(self, server, ident, msg):
        value = self.ios[msg.port].analog_value
        return analog.Reply(msg.port, value)

    @_command(analog.Subscribe)
    def analog_command_subscribe(self, server, ident, msg):
        self.ios[msg.port].subscribe(server, ident, msg.__class__, msg.subscription)
        return ack.Acknowledgement()

    @_command(digital.Request)
    def digital_request(self, server, ident, msg):
        value = self.ios[msg.port].digital_value
        return digital.Reply(msg.port, value)

    @_command(motor.Action)
    def motor_action(self, server, ident, msg):
        # if msg.relative is not None or msg.absolute is not None:
        #     # this action will end with a state update
        #     def cb(port, state):
        #         server.send_async(ident, motor.StateUpdate(port, state))
        #     self.motor_cb[msg.port] = cb
        self.motors[msg.port].action(msg.state, msg.amount, msg.reached_state, msg.relative, msg.absolute)
        return ack.Acknowledgement()

    @_command(motor.CommandRequest)
    def motor_command_request(self, server, ident, msg):
        command = self.motors[msg.port].command
        try:
            state, amount = command
        except TypeError:
            raise FailedCommandError("no command executed yet")
        else:
            return motor.CommandReply(msg.port, state, amount)

    @_command(motor.StateRequest)
    def motor_state_request(self, server, ident, msg):
        velocity, position = self.motors[msg.port].state
        return motor.StateReply(msg.port, velocity, position)

    # def motor_state_update(self, port, state):
    #     if port in self.motor_cb:
    #         self.motor_cb[port](port, state)
    #         del self.motor_cb[port]

    @_command(motor.SetPositionAction)
    def motor_set_position_action(self, server, ident, msg):
        self.motors[msg.port].set_position(msg.position)
        return ack.Acknowledgement()

    @_command(servo.Action)
    def servo_action(self, server, ident, msg):
        self.servos[msg.port].action(msg.active, msg.position)
        return ack.Acknowledgement()

    @_command(servo.CommandRequest)
    def servo_command_request(self, server, ident, msg):
        command = self.servos[msg.port].command
        try:
            active, position = command
        except TypeError:
            raise FailedCommandError("no command executed yet")
        else:
            return servo.CommandReply(msg.port, active, position)
