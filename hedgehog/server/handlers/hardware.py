from typing import Any, Callable, Dict, Tuple, Type

from hedgehog.protocol import Header, Message
from hedgehog.protocol.errors import UnsupportedCommandError, FailedCommandError
from hedgehog.protocol.messages import ack, io, analog, digital, motor, servo
from hedgehog.protocol.proto.subscription_pb2 import Subscription
from hedgehog.utils.zmq.timer import TimerDefinition

from . import CommandHandler, command_handlers
from ..hardware import HardwareAdapter
from ..hedgehog_server import HedgehogServerActor


class SubscriptionInfo(object):
    def __init__(self, server: HedgehogServerActor, ident: Header, subscription: Subscription,
                 handler: Callable[['SubscriptionInfo'], None]) -> None:
        self.server = server
        self.ident = ident
        self.subscription = subscription
        self.timer = server.timer.register(subscription.timeout / 1000, lambda: handler(self))
        self.counter = 0

    def send_async(self, *msgs: Message) -> None:
        self.server.send_async(self.ident, *msgs)

    def unregister(self) -> None:
        self.server.timer.unregister(self.timer)


class _HWHandler(object):
    def __init__(self, adapter: HardwareAdapter) -> None:
        self.adapter = adapter
        self.subscriptions = {}  # type: Dict[Tuple[Header, Type[Message], int], SubscriptionInfo]
        self.update_handlers = {}  # type: Dict[Type[Message], Callable[[SubscriptionInfo], None]]

    def subscribe(self, server: HedgehogServerActor, ident: Header, msg: Type[Message], subscription: Subscription) -> None:
        # TODO incomplete
        key = (ident, msg, subscription.timeout)

        if subscription.subscribe:
            if key not in self.subscriptions:
                info = SubscriptionInfo(server, ident, subscription, self.update_handlers[msg])
                self.subscriptions[key] = info
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
                    info.unregister()
                    del self.subscriptions[key]


class _IOHandler(_HWHandler):
    def __init__(self, adapter: HardwareAdapter, port: int) -> None:
        super(_IOHandler, self).__init__(adapter)
        self.port = port
        self.command = None  # type: Tuple[int]

        self.update_handlers[io.CommandSubscribe] = self._command_update

    def action(self, flags: int) -> None:
        self.adapter.set_io_state(self.port, flags)
        self.command = flags,

    @property
    def analog_value(self) -> int:
        return self.adapter.get_analog(self.port)

    @property
    def digital_value(self) -> bool:
        return self.adapter.get_digital(self.port)

    def _command_update(self, info: SubscriptionInfo) -> None:
        flags, = self.command
        info.send_async(io.CommandUpdate(self.port, flags, info.subscription))


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
