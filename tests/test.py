import unittest
import zmq
from hedgehog.protocol import sockets
from hedgehog.protocol.messages import analog, digital, motor, servo
from hedgehog.server import HedgehogServer
from hedgehog.server.simulator import SimulatorCommandHandler


class TestSimulator(unittest.TestCase):
    def test_analog_request(self):
        context = zmq.Context()

        controller = HedgehogServer('tcp://*:5555', SimulatorCommandHandler(), context=context)
        controller.start()

        socket = context.socket(zmq.DEALER)
        socket.connect('tcp://localhost:5555')
        socket = sockets.DealerWrapper(socket)

        socket.send(analog.Request(0))
        update = socket.recv()
        self.assertEqual(update.port, 0)
        self.assertEqual(update.value, 0)

        controller.close()

    def test_digital_request(self):
        context = zmq.Context()

        controller = HedgehogServer('tcp://*:5556', SimulatorCommandHandler(), context=context)
        controller.start()

        socket = context.socket(zmq.DEALER)
        socket.connect('tcp://localhost:5556')
        socket = sockets.DealerWrapper(socket)

        socket.send(digital.Request(0))
        update = socket.recv()
        self.assertEqual(update.port, 0)
        self.assertEqual(update.value, False)

        controller.close()

    def test_motor_request(self):
        context = zmq.Context()

        controller = HedgehogServer('tcp://*:5557', SimulatorCommandHandler(), context=context)
        controller.start()

        socket = context.socket(zmq.DEALER)
        socket.connect('tcp://localhost:5557')
        socket = sockets.DealerWrapper(socket)

        socket.send(motor.Request(0))
        update = socket.recv()
        self.assertEqual(update.port, 0)
        self.assertEqual(update.velocity, 0)
        self.assertEqual(update.position, 0)

        controller.close()


if __name__ == '__main__':
    unittest.main()
