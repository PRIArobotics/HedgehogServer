import zmq
import threading
from collections import OrderedDict
from hedgehog.protocol import messages, utils


class HedgehogSimulator(threading.Thread):
    def __init__(self, endpoint, context=None):
        super(HedgehogSimulator, self).__init__()
        self.context = context or zmq.Context.instance()
        self.endpoint = endpoint
        self.killer = utils.Killer(context=self.context)

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
                    msg = messages.parse(payload)
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
        msg = messages.AnalogUpdate(response)
        socket.send_multipart([ident, msg.SerializeToString()])
