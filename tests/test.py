import unittest
import zmq
from hedgehog.protocol import sockets
from hedgehog.protocol.messages import ack, io, analog, digital, motor, servo, process
from hedgehog.server import HedgehogServer, simulator
from hedgehog.server.process import Process


class TestSimulator(unittest.TestCase):
    def test_multipart(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.REQ)
        socket.connect('inproc://controller')
        socket = sockets.ReqWrapper(socket)

        socket.send_multipart([analog.Request(0), digital.Request(0)])
        update = socket.recv_multipart()
        self.assertEqual(update[0], analog.Update(0, 0))
        self.assertEqual(update[1], digital.Update(0, False))

        controller.close()

    def test_io_state_action(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.REQ)
        socket.connect('inproc://controller')
        socket = sockets.ReqWrapper(socket)

        socket.send(io.StateAction(0, io.ANALOG_PULLDOWN))
        response = socket.recv()
        self.assertEqual(response, ack.Acknowledgement())

        # send an invalid command
        action = io.StateAction(0, 0)
        action.flags = io.OUTPUT | io.ANALOG
        socket.send(action)
        response = socket.recv()
        self.assertEqual(type(response), ack.Acknowledgement)
        self.assertEqual(response.code, ack.INVALID_COMMAND)

        controller.close()

    def test_analog_request(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.REQ)
        socket.connect('inproc://controller')
        socket = sockets.ReqWrapper(socket)

        socket.send(analog.Request(0))
        update = socket.recv()
        self.assertEqual(update, analog.Update(0, 0))

        controller.close()

    def test_digital_request(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.REQ)
        socket.connect('inproc://controller')
        socket = sockets.ReqWrapper(socket)

        socket.send(digital.Request(0))
        update = socket.recv()
        self.assertEqual(update, digital.Update(0, False))

        controller.close()

    def test_motor_action(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.REQ)
        socket.connect('inproc://controller')
        socket = sockets.ReqWrapper(socket)

        socket.send(motor.Action(0, motor.POWER))
        response = socket.recv()
        self.assertEqual(response, ack.Acknowledgement())

        # send an invalid command
        action = motor.Action(0, motor.BRAKE)
        action.relative = 100
        socket.send(action)
        response = socket.recv()
        self.assertEqual(type(response), ack.Acknowledgement)
        self.assertEqual(response.code, ack.INVALID_COMMAND)

        controller.close()

    def test_motor_request(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.REQ)
        socket.connect('inproc://controller')
        socket = sockets.ReqWrapper(socket)

        socket.send(motor.Request(0))
        update = socket.recv()
        self.assertEqual(update, motor.Update(0, 0, 0))

        controller.close()

    def test_motor_set_position_action(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.REQ)
        socket.connect('inproc://controller')
        socket = sockets.ReqWrapper(socket)

        socket.send(motor.SetPositionAction(0, 0))
        response = socket.recv()
        self.assertEqual(response, ack.Acknowledgement())

        controller.close()

    def test_servo_action(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.REQ)
        socket.connect('inproc://controller')
        socket = sockets.ReqWrapper(socket)

        socket.send(servo.Action(0, True, 0))
        response = socket.recv()
        self.assertEqual(response, ack.Acknowledgement())

        controller.close()

    def test_process_execute_request_echo(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.DEALER)
        socket.connect('inproc://controller')
        socket = sockets.DealerRouterWrapper(socket)

        socket.send([], process.ExecuteRequest('echo', 'asdf'))
        _, response = socket.recv()
        self.assertEqual(type(response), process.ExecuteReply)
        pid = response.pid

        output = {
            process.STDOUT: [],
            process.STDERR: [],
        }

        open = 2
        while open > 0:
            _, msg = socket.recv()
            self.assertEqual(type(msg), process.StreamUpdate)
            self.assertEqual(msg.pid, pid)
            output[msg.fileno].append(msg.chunk)
            if msg.chunk == b'':
                open -= 1
        _, msg = socket.recv()
        self.assertEqual(msg, process.ExitUpdate(pid, 0))

        output = {fileno: b''.join(chunks) for fileno, chunks in output.items()}

        self.assertEqual(output[process.STDOUT], b'asdf\n')
        self.assertEqual(output[process.STDERR], b'')

        controller.close()

    def test_process_execute_request_cat(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.DEALER)
        socket.connect('inproc://controller')
        socket = sockets.DealerRouterWrapper(socket)

        socket.send([], process.ExecuteRequest('cat'))
        _, response = socket.recv()
        self.assertEqual(type(response), process.ExecuteReply)
        pid = response.pid

        output = {
            process.STDOUT: [],
            process.STDERR: [],
        }

        socket.send([], process.StreamAction(pid, process.STDIN, b'asdf'))
        _, response = socket.recv()
        self.assertEqual(response, ack.Acknowledgement())
        socket.send([], process.StreamAction(pid, process.STDIN, b''))
        _, response = socket.recv()
        self.assertEqual(response, ack.Acknowledgement())

        open = 2
        while open > 0:
            _, msg = socket.recv()
            self.assertEqual(type(msg), process.StreamUpdate)
            self.assertEqual(msg.pid, pid)
            output[msg.fileno].append(msg.chunk)
            if msg.chunk == b'':
                open -= 1
        _, msg = socket.recv()
        self.assertEqual(msg, process.ExitUpdate(pid, 0))

        output = {fileno: b''.join(chunks) for fileno, chunks in output.items()}

        self.assertEqual(output[process.STDOUT], b'asdf')
        self.assertEqual(output[process.STDERR], b'')

        controller.close()

    def test_process_execute_request_pwd(self):
        context = zmq.Context()

        controller = HedgehogServer('inproc://controller', simulator.handler(), context=context)
        controller.start()

        socket = context.socket(zmq.DEALER)
        socket.connect('inproc://controller')
        socket = sockets.DealerRouterWrapper(socket)

        socket.send([], process.ExecuteRequest('pwd', working_dir='/'))
        _, response = socket.recv()
        self.assertEqual(type(response), process.ExecuteReply)
        pid = response.pid

        output = {
            process.STDOUT: [],
            process.STDERR: [],
        }

        open = 2
        while open > 0:
            _, msg = socket.recv()
            self.assertEqual(type(msg), process.StreamUpdate)
            self.assertEqual(msg.pid, pid)
            output[msg.fileno].append(msg.chunk)
            if msg.chunk == b'':
                open -= 1
        _, msg = socket.recv()
        self.assertEqual(msg, process.ExitUpdate(pid, 0))

        output = {fileno: b''.join(chunks) for fileno, chunks in output.items()}

        self.assertEqual(output[process.STDOUT], b'/\n')
        self.assertEqual(output[process.STDERR], b'')

        controller.close()


def collect_outputs(proc):
    output = {process.STDOUT: [], process.STDERR: []}

    msg = proc.read()
    while msg is not None:
        fileno, msg = msg
        if msg != b'':
            output[fileno].append(msg)
        msg = proc.read()
    proc.socket.close()
    status = proc.status

    return status, b''.join(output[process.STDOUT]), b''.join(output[process.STDERR])


class TestProcess(unittest.TestCase):
    def test_cat(self):
        proc = Process('cat')

        proc.write(process.STDIN, b'as ')
        proc.write(process.STDIN, b'df')
        proc.write(process.STDIN)

        status, out, err = collect_outputs(proc)
        self.assertEqual(status, 0)
        self.assertEqual(out, b'as df')
        self.assertEqual(err, b'')

    def test_echo(self):
        proc = Process('echo', 'as', 'df')

        proc.write(process.STDIN)

        status, out, err = collect_outputs(proc)
        self.assertEqual(status, 0)
        self.assertEqual(out, b'as df\n')
        self.assertEqual(err, b'')

    def test_pwd(self):
        proc = Process('pwd', cwd='/')

        proc.write(process.STDIN)

        status, out, err = collect_outputs(proc)
        self.assertEqual(status, 0)
        self.assertEqual(out, b'/\n')
        self.assertEqual(err, b'')


if __name__ == '__main__':
    unittest.main()
