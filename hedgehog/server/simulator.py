from hedgehog.protocol import messages
from hedgehog.protocol.messages import analog, digital, motor, servo


class SimulatorCommandHandler:
    def analog_request(self, socket, ident, msg):
        socket.send(ident, messages.analog.Update(msg.port, 0))

    def analog_state_action(self, socket, ident, msg):
        # TODO set analog pullup
        pass

    def digital_request(self, socket, ident, msg):
        socket.send(ident, messages.digital.Update(msg.port, False))

    def digital_state_action(self, socket, ident, msg):
        # TODO set digital pullup, output
        pass

    def digital_action(self, socket, ident, msg):
        # TODO set digital pullup, output
        pass

    def motor_action(self, socket, ident, msg):
        # TODO set motor action
        pass

    def motor_request(self, socket, ident, msg):
        socket.send(ident, messages.motor.Update(msg.port, 0, 0))

    def motor_set_position_action(self, socket, ident, msg):
        # TODO set motor position
        pass

    def servo_action(self, socket, ident, msg):
        # TODO set servo position
        pass

    def servo_state_action(self, socket, ident, msg):
        # TODO set servo active
        pass


def main():
    import zmq
    from hedgehog.server import HedgehogServer

    context = zmq.Context.instance()

    simulator = HedgehogServer('tcp://*:5555', SimulatorCommandHandler(), context=context)
    simulator.start()


if __name__ == '__main__':
    main()
