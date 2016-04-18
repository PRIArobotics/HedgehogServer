import unittest
import zmq
from hedgehog import proto
from hedgehog.simulator.controller import HedgehogController


class TestSimulator(unittest.TestCase):
    def test_analog_request(self):
        context = zmq.Context.instance()

        controller = HedgehogController('tcp://*:5555', context=context)
        controller.start()

        socket = context.socket(zmq.DEALER)
        socket.identity = b'A'
        socket.connect('tcp://localhost:5555')

        msg = proto.AnalogRequest([0, 1])
        socket.send(msg.SerializeToString())

        payload = socket.recv()
        msg = proto.parse(payload)
        self.assertEqual(msg.analog_update.sensors, {0: 0, 1: 0})

        controller.kill()


if __name__ == '__main__':
    unittest.main()
