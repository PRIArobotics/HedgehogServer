import zmq
import threading
from collections import OrderedDict
from hedgehog.protocol import messages, sockets, utils
from hedgehog.protocol.messages import analog, digital, motor, servo


class HedgehogSimulator(threading.Thread):
    def __init__(self, endpoint, context=None):
        super(HedgehogSimulator, self).__init__()
        self.context = context or zmq.Context.instance()
        self.endpoint = endpoint
        self.killer = utils.Killer()

    def close(self):
        self.killer.kill()

    def run(self):
        socket = self.context.socket(zmq.ROUTER)
        socket.bind(self.endpoint)
        socket = sockets.RouterWrapper(socket)
        killer = self.killer.connect_receiver()

        while True:
            pollin, _, _ = zmq.select([socket.socket, killer], [], [])
            if pollin:
                if socket.socket in pollin:
                    ident, msg = socket.recv()
                    try:
                        handler = getattr(self, msg._command_oneof)
                    except AttributeError:
                        # TODO handle unknown commands
                        print(msg._command_oneof + ': unknown command')
                        pass
                    else:
                        handler(socket, ident, msg)
                if killer in pollin:
                    break

        socket.close()
        killer.close()

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
