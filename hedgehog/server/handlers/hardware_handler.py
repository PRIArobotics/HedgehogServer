from hedgehog.protocol.messages import analog, digital, motor, servo
from hedgehog.server.handlers import CommandHandler, command_handlers

from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter

class HardwareHandler(CommandHandler):
    _handlers, _command = command_handlers()

    def __init__(self, adapter):
        super().__init__()
        self.adapter = SimulatedHardwareAdapter()

    @_command(analog.Request)
    def analog_request(self, server, ident, msg):
        value = self.adapter.get_analog(msg.port)
        server.socket.send(ident, analog.Update(msg.port, value))

    @_command(analog.StateAction)
    def analog_state_action(self, server, ident, msg):
        self.adapter.set_analog_state(msg.port, msg.pullup)

    @_command(digital.Request)
    def digital_request(self, server, ident, msg):
        value = self.adapter.get_digital(msg.port)
        server.socket.send(ident, digital.Update(msg.port, value))

    @_command(digital.StateAction)
    def digital_state_action(self, server, ident, msg):
        self.adapter.set_digital_state(msg.port, msg.pullup, msg.output)

    @_command(digital.Action)
    def digital_action(self, server, ident, msg):
        self.adapter.set_digital(msg.port, msg.level)

    @_command(motor.Action)
    def motor_action(self, server, ident, msg):
        self.adapter.set_motor(msg.port, msg.state, msg.amount, msg.reached_state, msg.relative, msg.absolute)

    @_command(motor.Request)
    def motor_request(self, server, ident, msg):
        velocity, position = self.adapter.get_motor(msg.port)
        server.socket.send(ident, motor.Update(msg.port, velocity, position))

    @_command(motor.SetPositionAction)
    def motor_set_position_action(self, server, ident, msg):
        self.adapter.set_motor_position(msg.port, msg.position)

    @_command(servo.Action)
    def servo_action(self, server, ident, msg):
        self.adapter.set_servo(msg.port, msg.position)

    @_command(servo.StateAction)
    def servo_state_action(self, server, ident, msg):
        self.adapter.set_servo_state(msg.port, msg.active)
