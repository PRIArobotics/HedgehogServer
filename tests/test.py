from typing import Callable, Type, Union

import unittest
import zmq
import signal
import time
from contextlib import contextmanager

from hedgehog.protocol import ClientSide
from hedgehog.protocol.messages import Message, ack, io, analog, digital, motor, servo, process
from hedgehog.protocol.sockets import ReqSocket, DealerRouterSocket
from hedgehog.server import handlers, HedgehogServer
from hedgehog.server.process import Process
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter


def handler() -> handlers.HandlerCallbackDict:
    adapter = SimulatedHardwareAdapter()
    return handlers.to_dict(HardwareHandler(adapter), ProcessHandler(adapter))


@contextmanager
def connectSimulatorReq(handlers: handlers.HandlerCallbackDict=None):
    if handlers is None:
        handlers = handler()

    ctx = zmq.Context()
    with HedgehogServer(ctx, 'inproc://controller', handlers):
        socket = ReqSocket(ctx, zmq.REQ, side=ClientSide)
        socket.connect('inproc://controller')

        yield socket


@contextmanager
def connectSimulatorDealer(handlers: handlers.HandlerCallbackDict=None):
    if handlers is None:
        handlers = handler()

    ctx = zmq.Context()
    with HedgehogServer(ctx, 'inproc://controller', handlers):
        socket = DealerRouterSocket(ctx, zmq.DEALER, side=ClientSide)
        socket.connect('inproc://controller')

        yield socket


class TestSimulator(unittest.TestCase):
    def assertMsgEqual(self, msg: Message, msg_class: Type[Message], **kwargs) -> None:
        self.assertEqual(type(msg), msg_class)
        for field, value in kwargs.items():
            self.assertEqual(getattr(msg, field), value)

    def assertNack(self, msg: Message, code: int, **kwargs) -> None:
        self.assertMsgEqual(msg, ack.Acknowledgement, code=code, **kwargs)

    def _check(self, expect: Union[int, type, Message, Callable[[Message], None]], **kwargs) -> Callable[[Message], None]:
        if isinstance(expect, int):
            code = expect  # type: int
            return lambda msg: self.assertNack(msg, code, **kwargs)
        elif isinstance(expect, type) and issubclass(expect, Message):
            msg_class = expect  # type: Type[Message]
            return lambda msg: self.assertMsgEqual(msg, msg_class, **kwargs)
        elif isinstance(expect, Message):
            rep = expect  # type: Message
            return lambda msg: self.assertEqual(msg, rep)
        else:
            check = expect  # type: Callable[[Message], None]
            return check

    def assertReplyReq(self, socket, req: Message,
                       rep: Union[int, type, Message, Callable[[Message], None]], **kwargs) -> Message:
        check = self._check(rep, **kwargs)

        socket.send_msg(req)
        response = socket.recv_msg()
        check(response)
        return response

    def test_multipart(self):
        with connectSimulatorReq() as socket:
            socket.send_msgs([analog.Request(0), digital.Request(0)])
            update = socket.recv_msgs()
            self.assertEqual(update[0], analog.Reply(0, 0))
            self.assertEqual(update[1], digital.Reply(0, False))

    def test_unsupported(self):
        from hedgehog.server import handlers
        from hedgehog.server.handlers.hardware import HardwareHandler
        from hedgehog.server.handlers.process import ProcessHandler
        from hedgehog.server.hardware import HardwareAdapter
        adapter = HardwareAdapter()
        _handlers = handlers.to_dict(HardwareHandler(adapter), ProcessHandler(adapter))

        with connectSimulatorReq(_handlers) as socket:
            self.assertReplyReq(socket, io.StateAction(0, io.INPUT_PULLDOWN), ack.UNSUPPORTED_COMMAND)

    def test_io_state_action(self):
        with connectSimulatorReq() as socket:
            self.assertReplyReq(socket, io.StateAction(0, io.INPUT_PULLDOWN), ack.Acknowledgement())

            # send an invalid command
            action = io.StateAction(0, 0)
            action.flags = io.OUTPUT | io.PULLDOWN
            self.assertReplyReq(socket, action, ack.INVALID_COMMAND)

    def test_analog_request(self):
        with connectSimulatorReq() as socket:
            self.assertReplyReq(socket, analog.Request(0), analog.Reply(0, 0))

    def test_digital_request(self):
        with connectSimulatorReq() as socket:
            self.assertReplyReq(socket, digital.Request(0), digital.Reply(0, False))

    def test_motor_action(self):
        with connectSimulatorReq() as socket:
            self.assertReplyReq(socket, motor.Action(0, motor.POWER), ack.Acknowledgement())

            # send an invalid command
            action = motor.Action(0, motor.BRAKE)
            action.relative = 100
            self.assertReplyReq(socket, action, ack.INVALID_COMMAND)

    def test_motor_request(self):
        with connectSimulatorReq() as socket:
            self.assertReplyReq(socket, motor.StateRequest(0), motor.StateReply(0, 0, 0))

    def test_motor_set_position_action(self):
        with connectSimulatorReq() as socket:
            self.assertReplyReq(socket, motor.SetPositionAction(0, 0), ack.Acknowledgement())

    def test_servo_action(self):
        with connectSimulatorReq() as socket:
            self.assertReplyReq(socket, servo.Action(0, True, 0), ack.Acknowledgement())

    def test_process_execute_request_echo(self):
        with connectSimulatorDealer() as socket:
            socket.send_msg([], process.ExecuteAction('echo', 'asdf'))
            _, response = socket.recv_msg()  # type: process.ExecuteReply
            self.assertMsgEqual(response, process.ExecuteReply)
            pid = response.pid

            output = {
                process.STDOUT: [],
                process.STDERR: [],
            }

            open = 2
            while open > 0:
                _, msg = socket.recv_msg()  # type: process.StreamUpdate
                self.assertMsgEqual(msg, process.StreamUpdate, pid=pid)
                output[msg.fileno].append(msg.chunk)
                if msg.chunk == b'':
                    open -= 1

            socket.send_msg([], process.StreamAction(pid, process.STDIN, b''))
            _, response = socket.recv_msg()
            self.assertEqual(response, ack.Acknowledgement())

            _, msg = socket.recv_msg()
            self.assertEqual(msg, process.ExitUpdate(pid, 0))

            output = {fileno: b''.join(chunks) for fileno, chunks in output.items()}

            self.assertEqual(output[process.STDOUT], b'asdf\n')
            self.assertEqual(output[process.STDERR], b'')

    def test_process_execute_request_cat(self):
        with connectSimulatorDealer() as socket:
            socket.send_msg([], process.ExecuteAction('cat'))
            _, response = socket.recv_msg()  # type: process.ExecuteReply
            self.assertMsgEqual(response, process.ExecuteReply)
            pid = response.pid

            output = {
                process.STDOUT: [],
                process.STDERR: [],
            }

            socket.send_msg([], process.StreamAction(pid, process.STDIN, b'asdf'))
            _, response = socket.recv_msg()
            self.assertEqual(response, ack.Acknowledgement())
            socket.send_msg([], process.StreamAction(pid, process.STDIN, b''))
            _, response = socket.recv_msg()
            self.assertEqual(response, ack.Acknowledgement())

            open = 2
            while open > 0:
                _, msg = socket.recv_msg()  # type: process.StreamUpdate
                self.assertMsgEqual(msg, process.StreamUpdate, pid=pid)
                output[msg.fileno].append(msg.chunk)
                if msg.chunk == b'':
                    open -= 1
            _, msg = socket.recv_msg()
            self.assertEqual(msg, process.ExitUpdate(pid, 0))

            output = {fileno: b''.join(chunks) for fileno, chunks in output.items()}

            self.assertEqual(output[process.STDOUT], b'asdf')
            self.assertEqual(output[process.STDERR], b'')

    def test_process_execute_request_pwd(self):
        with connectSimulatorDealer() as socket:
            socket.send_msg([], process.ExecuteAction('pwd', working_dir='/'))
            _, response = socket.recv_msg()  # type: process.ExecuteReply
            self.assertMsgEqual(response, process.ExecuteReply)
            pid = response.pid

            output = {
                process.STDOUT: [],
                process.STDERR: [],
            }

            open = 2
            while open > 0:
                _, msg = socket.recv_msg()  # type: process.StreamUpdate
                self.assertMsgEqual(msg, process.StreamUpdate, pid=pid)
                output[msg.fileno].append(msg.chunk)
                if msg.chunk == b'':
                    open -= 1

            socket.send_msg([], process.StreamAction(pid, process.STDIN, b''))
            _, response = socket.recv_msg()
            self.assertEqual(response, ack.Acknowledgement())

            _, msg = socket.recv_msg()
            self.assertEqual(msg, process.ExitUpdate(pid, 0))

            output = {fileno: b''.join(chunks) for fileno, chunks in output.items()}

            self.assertEqual(output[process.STDOUT], b'/\n')
            self.assertEqual(output[process.STDERR], b'')

    def test_process_signal_sleep(self):
        with connectSimulatorDealer() as socket:
            socket.send_msg([], process.ExecuteAction('sleep', '1'))
            _, response = socket.recv_msg()  # type: process.ExecuteReply
            self.assertMsgEqual(response, process.ExecuteReply)
            pid = response.pid

            socket.send_msg([], process.StreamAction(pid, process.STDIN, b''))
            _, response = socket.recv_msg()
            self.assertEqual(response, ack.Acknowledgement())

            socket.send_msg([], process.SignalAction(pid, signal.SIGINT))
            _, response = socket.recv_msg()
            self.assertEqual(response, ack.Acknowledgement())

            open = 2
            while open > 0:
                _, msg = socket.recv_msg()  # type: process.StreamUpdate
                self.assertMsgEqual(msg, process.StreamUpdate, pid=pid)
                if msg.chunk == b'':
                    open -= 1

            _, msg = socket.recv_msg()
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
