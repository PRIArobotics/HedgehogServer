import zmq
import threading
from collections import OrderedDict
from hedgehog.protocol import messages, sockets, utils


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
        socket.send(ident, messages.AnalogUpdate(response))
