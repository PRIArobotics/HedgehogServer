from hedgehog.protocol.messages import analog, digital, motor, servo
from hedgehog.server.handlers import CommandHandler, command_handlers


class SimulatorHandler(CommandHandler):
    _handlers, _command = command_handlers()

    @_command(analog.Request)
    def analog_request(self, server, ident, msg):
        server.socket.send(ident, analog.Update(msg.port, 0))

    @_command(analog.StateAction)
    def analog_state_action(self, server, ident, msg):
        # TODO set analog pullup
        pass

    @_command(digital.Request)
    def digital_request(self, server, ident, msg):
        server.socket.send(ident, digital.Update(msg.port, False))

    @_command(digital.StateAction)
    def digital_state_action(self, server, ident, msg):
        # TODO set digital pullup, output
        pass

    @_command(digital.Action)
    def digital_action(self, server, ident, msg):
        # TODO set digital pullup, output
        pass

    @_command(motor.Action)
    def motor_action(self, server, ident, msg):
        # TODO set motor action
        pass

    @_command(motor.Request)
    def motor_request(self, server, ident, msg):
        server.socket.send(ident, motor.Update(msg.port, 0, 0))

    @_command(motor.SetPositionAction)
    def motor_set_position_action(self, server, ident, msg):
        # TODO set motor position
        pass

    @_command(servo.Action)
    def servo_action(self, server, ident, msg):
        # TODO set servo position
        pass

    @_command(servo.StateAction)
    def servo_state_action(self, server, ident, msg):
        # TODO set servo active
        pass
