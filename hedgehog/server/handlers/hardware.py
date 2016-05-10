from hedgehog.protocol.messages import ack, io, analog, digital, motor, servo
from hedgehog.server.handlers import CommandHandler, command_handlers


class HardwareHandler(CommandHandler):
    _handlers, _command = command_handlers()

    def __init__(self, adapter):
        super().__init__()
        self.motor_cb = {}
        self.adapter = adapter
        self.adapter.motor_state_update_cb = self.motor_state_update

    @_command(io.StateAction)
    def analog_state_action(self, server, ident, msg):
        self.adapter.set_io_state(msg.port, msg.flags)
        return ack.Acknowledgement()

    @_command(analog.Request)
    def analog_request(self, server, ident, msg):
        value = self.adapter.get_analog(msg.port)
        return analog.Update(msg.port, value)

    @_command(digital.Request)
    def digital_request(self, server, ident, msg):
        value = self.adapter.get_digital(msg.port)
        return digital.Update(msg.port, value)

    @_command(motor.Action)
    def motor_action(self, server, ident, msg):
        if msg.relative is not None or msg.absolute is not None:
            # this action will end with a state update
            def cb(port, state):
                server.socket.send(ident, motor.StateUpdate(port, state))
            self.motor_cb[msg.port] = cb
        self.adapter.set_motor(msg.port, msg.state, msg.amount, msg.reached_state, msg.relative, msg.absolute)
        return ack.Acknowledgement()

    @_command(motor.Request)
    def motor_request(self, server, ident, msg):
        velocity, position = self.adapter.get_motor(msg.port)
        return motor.Update(msg.port, velocity, position)

    def motor_state_update(self, port, state):
        if port in self.motor_cb:
            self.motor_cb[port](port, state)
            del self.motor_cb[port]

    @_command(motor.SetPositionAction)
    def motor_set_position_action(self, server, ident, msg):
        self.adapter.set_motor_position(msg.port, msg.position)
        return ack.Acknowledgement()

    @_command(servo.Action)
    def servo_action(self, server, ident, msg):
        self.adapter.set_servo(msg.port, msg.position)
        return ack.Acknowledgement()

    @_command(servo.StateAction)
    def servo_state_action(self, server, ident, msg):
        self.adapter.set_servo_state(msg.port, msg.active)
        return ack.Acknowledgement()
