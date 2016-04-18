import zmq
import threading
from collections import OrderedDict
import hedgehog.proto


class Killer:
    def __init__(self, context=None):
        self.context = context or zmq.Context.instance()
        self.endpoint = 'inproc://killer'
        self.sender = self.context.socket(zmq.PAIR)
        self.sender.bind(self.endpoint)

    def connect_receiver(self):
        receiver = self.context.socket(zmq.PAIR)
        receiver.connect(self.endpoint)
        return receiver

    def kill(self):
        self.sender.send(b'')
        self.sender.close()


class HedgehogController(threading.Thread):
    def __init__(self, endpoint, context=None):
        super(HedgehogController, self).__init__()
        self.context = context or zmq.Context.instance()
        self.endpoint = endpoint
        self.killer = Killer(context=self.context)

    def kill(self):
        self.killer.kill()

    def run(self):
        socket = self.context.socket(zmq.ROUTER)
        socket.bind(self.endpoint)
        killer = self.killer.connect_receiver()

        while True:
            pollin, _, _ = zmq.select([socket, killer], [], [])
            if pollin:
                if socket in pollin:
                    ident, payload = socket.recv_multipart()
                    msg = hedgehog.proto.parse(payload)
                    try:
                        handler = getattr(self, msg.WhichOneof('command'))
                    except AttributeError:
                        # TODO handle unknown commands
                        pass
                    else:
                        handler(socket, ident, msg)
                if killer in pollin:
                    break

        socket.close()
        killer.close()


    def analog_request(self, socket, ident, msg):
        response = OrderedDict()
        for sensor in msg.analog_request.sensors:
            response[sensor] = 0
        msg = hedgehog.proto.AnalogUpdate(response)
        socket.send_multipart([ident, msg.SerializeToString()])
