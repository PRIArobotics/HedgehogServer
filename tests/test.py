import unittest
import zmq
from hedgehog.protocol import messages, sockets
from hedgehog.simulator import HedgehogSimulator


class TestSimulator(unittest.TestCase):
    def test_analog_request(self):
        context = zmq.Context.instance()

        controller = HedgehogSimulator('tcp://*:5555', context=context)
        controller.start()

        socket = context.socket(zmq.DEALER)
        socket.connect('tcp://localhost:5555')
        socket = sockets.DealerWrapper(socket)

        socket.send(messages.AnalogRequest([0, 1]))
        sensors = socket.recv().analog_update.sensors
        self.assertEqual(sensors, {0: 0, 1: 0})

        controller.close()


if __name__ == '__main__':
    unittest.main()
