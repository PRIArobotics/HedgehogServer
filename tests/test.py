import unittest
import zmq
from hedgehog.protocol import sockets
from hedgehog.protocol.messages import analog, digital, motor, servo
from hedgehog.server import HedgehogServer, process
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


def collect_outputs(*sockets):
    output = {socket: [] for socket in sockets}

    poller = zmq.Poller()
    for socket in sockets:
        poller.register(socket)

    while len(poller.sockets) > 0:
        for socket, _ in poller.poll():
            msg = socket.recv()
            if msg != b'':
                output[socket].append(msg)
            else:
                poller.unregister(socket)
                socket.close()

    return (b''.join(output[socket]) for socket in sockets)


class TestProcess(unittest.TestCase):
    def test_cat(self):
        proc, stdin, stdout, stderr = process.run('cat')

        stdin.send(b'as ')
        stdin.send(b'df')
        stdin.send(b'')
        stdin.close()

        out, err = collect_outputs(stdout, stderr)
        self.assertEqual(out, b'as df')
        self.assertEqual(err, b'')

    def test_echo(self):
        proc, stdin, stdout, stderr = process.run('echo', 'as', 'df')

        stdin.send(b'')
        stdin.close()

        out, err = collect_outputs(stdout, stderr)
        self.assertEqual(out, b'as df\n')
        self.assertEqual(err, b'')


if __name__ == '__main__':
    unittest.main()
