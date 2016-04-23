import unittest
import zmq
from hedgehog.protocol import sockets
from hedgehog.protocol.messages import analog
from hedgehog.simulator import HedgehogSimulator


class TestSimulator(unittest.TestCase):
    def test_analog_request(self):
        context = zmq.Context()

        controller = HedgehogSimulator('tcp://*:5555', context=context)
        controller.start()

        socket = context.socket(zmq.DEALER)
        socket.connect('tcp://localhost:5555')
        socket = sockets.DealerWrapper(socket)

        socket.send(analog.Request(0))
        update = socket.recv()
        self.assertEqual(update.port, 0)
        self.assertEqual(update.value, 0)

        controller.close()


if __name__ == '__main__':
    unittest.main()
