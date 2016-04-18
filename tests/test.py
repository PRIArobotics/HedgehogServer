import unittest
import zmq
from hedgehog.protocol import messages
from hedgehog.simulator import HedgehogSimulator


class TestSimulator(unittest.TestCase):
    def test_analog_request(self):
        context = zmq.Context.instance()

        controller = HedgehogSimulator('tcp://*:5555', context=context)
        controller.start()

        socket = context.socket(zmq.DEALER)
        socket.identity = b'A'
        socket.connect('tcp://localhost:5555')

        msg = messages.AnalogRequest([0, 1])
        socket.send(msg.SerializeToString())

        payload = socket.recv()
        msg = messages.parse(payload)
        self.assertEqual(msg.analog_update.sensors, {0: 0, 1: 0})

        controller.kill()


if __name__ == '__main__':
    unittest.main()
