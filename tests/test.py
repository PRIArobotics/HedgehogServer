import unittest
import zmq
import signal
import time

from hedgehog.protocol import sockets
from hedgehog.protocol.messages import ack, io, analog, digital, motor, servo, process
from hedgehog.server import handlers, HedgehogServer
from hedgehog.server.process import Process
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter


def handler():
    return handlers.to_dict(HardwareHandler(SimulatedHardwareAdapter()), ProcessHandler())


class TestSimulator(unittest.TestCase):
    def test_multipart(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.REQ)
            socket.connect('inproc://controller')
            socket = sockets.ReqWrapper(socket)

            socket.send_multipart([analog.Request(0), digital.Request(0)])
            update = socket.recv_multipart()
            self.assertEqual(update[0], analog.Update(0, 0))
            self.assertEqual(update[1], digital.Update(0, False))

    def test_unsupported(self):
        ctx = zmq.Context()

        from hedgehog.server import handlers
        from hedgehog.server.handlers.hardware import HardwareHandler
        from hedgehog.server.handlers.process import ProcessHandler
        from hedgehog.server.hardware import HardwareAdapter
        handlers = handlers.to_dict(HardwareHandler(HardwareAdapter()), ProcessHandler())
        with HedgehogServer(ctx, 'inproc://controller', handlers):
            socket = ctx.socket(zmq.REQ)
            socket.connect('inproc://controller')
            socket = sockets.ReqWrapper(socket)

            socket.send(io.StateAction(0, io.INPUT_PULLDOWN))
            response = socket.recv()
            self.assertEqual(type(response), ack.Acknowledgement)
            self.assertEqual(response.code, ack.UNSUPPORTED_COMMAND)

    def test_io_state_action(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.REQ)
            socket.connect('inproc://controller')
            socket = sockets.ReqWrapper(socket)

            socket.send(io.StateAction(0, io.INPUT_PULLDOWN))
            response = socket.recv()
            self.assertEqual(response, ack.Acknowledgement())

            # send an invalid command
            action = io.StateAction(0, 0)
            action.flags = io.OUTPUT | io.PULLDOWN
            socket.send(action)
            response = socket.recv()
            self.assertEqual(type(response), ack.Acknowledgement)
            self.assertEqual(response.code, ack.INVALID_COMMAND)

    def test_analog_request(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.REQ)
            socket.connect('inproc://controller')
            socket = sockets.ReqWrapper(socket)

            socket.send(analog.Request(0))
            update = socket.recv()
            self.assertEqual(update, analog.Update(0, 0))

    def test_digital_request(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.REQ)
            socket.connect('inproc://controller')
            socket = sockets.ReqWrapper(socket)

            socket.send(digital.Request(0))
            update = socket.recv()
            self.assertEqual(update, digital.Update(0, False))

    def test_motor_action(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.REQ)
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

    def test_motor_request(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.REQ)
            socket.connect('inproc://controller')
            socket = sockets.ReqWrapper(socket)

            socket.send(motor.Request(0))
            update = socket.recv()
            self.assertEqual(update, motor.Update(0, 0, 0))

    def test_motor_set_position_action(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.REQ)
            socket.connect('inproc://controller')
            socket = sockets.ReqWrapper(socket)

            socket.send(motor.SetPositionAction(0, 0))
            response = socket.recv()
            self.assertEqual(response, ack.Acknowledgement())

    def test_servo_action(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.REQ)
            socket.connect('inproc://controller')
            socket = sockets.ReqWrapper(socket)

            socket.send(servo.Action(0, True, 0))
            response = socket.recv()
            self.assertEqual(response, ack.Acknowledgement())

    def test_process_execute_request_echo(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.DEALER)
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

            socket.send([], process.StreamAction(pid, process.STDIN, b''))
            _, response = socket.recv()
            self.assertEqual(response, ack.Acknowledgement())

            _, msg = socket.recv()
            self.assertEqual(msg, process.ExitUpdate(pid, 0))

            output = {fileno: b''.join(chunks) for fileno, chunks in output.items()}

            self.assertEqual(output[process.STDOUT], b'asdf\n')
            self.assertEqual(output[process.STDERR], b'')

    def test_process_execute_request_cat(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.DEALER)
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

    def test_process_execute_request_pwd(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.DEALER)
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

            socket.send([], process.StreamAction(pid, process.STDIN, b''))
            _, response = socket.recv()
            self.assertEqual(response, ack.Acknowledgement())

            _, msg = socket.recv()
            self.assertEqual(msg, process.ExitUpdate(pid, 0))

            output = {fileno: b''.join(chunks) for fileno, chunks in output.items()}

            self.assertEqual(output[process.STDOUT], b'/\n')
            self.assertEqual(output[process.STDERR], b'')

    def test_process_signal_sleep(self):
        ctx = zmq.Context()

        with HedgehogServer(ctx, 'inproc://controller', handler()):
            socket = ctx.socket(zmq.DEALER)
            socket.connect('inproc://controller')
            socket = sockets.DealerRouterWrapper(socket)

            socket.send([], process.ExecuteRequest('sleep', '1'))
            _, response = socket.recv()
            self.assertEqual(type(response), process.ExecuteReply)
            pid = response.pid

            socket.send([], process.StreamAction(pid, process.STDIN, b''))
            _, response = socket.recv()
            self.assertEqual(response, ack.Acknowledgement())

            socket.send([], process.SignalAction(pid, signal.SIGINT))
            _, response = socket.recv()
            self.assertEqual(response, ack.Acknowledgement())

            open = 2
            while open > 0:
                _, msg = socket.recv()
                self.assertEqual(type(msg), process.StreamUpdate)
                self.assertEqual(msg.pid, pid)
                if msg.chunk == b'':
                    open -= 1

            _, msg = socket.recv()
            self.assertEqual(msg, process.ExitUpdate(pid, -signal.SIGINT))


def collect_outputs(proc):
    output = {process.STDOUT: [], process.STDERR: []}

    msg = proc.read()
    while msg is not None:
        fileno, msg = msg
        if msg != b'':
            output[fileno].append(msg)
        msg = proc.read()
    proc.socket.close()

    return proc.returncode, b''.join(output[process.STDOUT]), b''.join(output[process.STDERR])


class TestProcess(unittest.TestCase):
    def test_cat(self):
        proc = Process('cat')

        proc.write(process.STDIN, b'as ')
        time.sleep(0.1)
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

    def test_signal_sleep(self):
        proc = Process('sleep', '1')

        proc.write(process.STDIN)

        proc.send_signal(signal.SIGINT)

        status, out, err = collect_outputs(proc)
        self.assertEqual(status, -2)
        self.assertEqual(out, b'')
        self.assertEqual(err, b'')


if __name__ == '__main__':
    unittest.main()
