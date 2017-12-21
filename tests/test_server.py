from typing import Any, Callable, Dict, List, Tuple, Type, Union

import pytest
import asyncio.selector_events
import zmq.asyncio
import signal
import time
from aiostream.context_utils import async_context_manager

from hedgehog.protocol import ClientSide
from hedgehog.protocol.messages import Message, ack, io, analog, digital, motor, servo, process
from hedgehog.protocol.proto.subscription_pb2 import Subscription
from hedgehog.protocol.async_sockets import ReqSocket, DealerRouterSocket
from hedgehog.server import handlers, HedgehogServer
from hedgehog.server.process import Process
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter


@pytest.fixture
def event_loop():
    loop = zmq.asyncio.ZMQEventLoop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


def handler() -> handlers.HandlerCallbackDict:
    adapter = SimulatedHardwareAdapter()
    return handlers.to_dict(HardwareHandler(adapter), ProcessHandler(adapter))


@async_context_manager
async def connectSimulatorReq(handlers: handlers.HandlerCallbackDict=None):
    if handlers is None:
        handlers = handler()

    ctx = zmq.asyncio.Context()
    async with HedgehogServer(ctx, 'inproc://controller', handlers):
        socket = ReqSocket(ctx, zmq.REQ, side=ClientSide)
        socket.connect('inproc://controller')

        yield socket

        socket.close()
    ctx.term()


@async_context_manager
async def connectSimulatorDealer(handlers: handlers.HandlerCallbackDict=None):
    if handlers is None:
        handlers = handler()

    ctx = zmq.asyncio.Context()
    async with HedgehogServer(ctx, 'inproc://controller', handlers):
        socket = DealerRouterSocket(ctx, zmq.DEALER, side=ClientSide)
        socket.connect('inproc://controller')

        yield socket

        socket.close()
    ctx.term()


def assertMsgEqual(msg: Message, msg_class: Type[Message], **kwargs) -> None:
    assert type(msg) == msg_class
    for field, value in kwargs.items():
        assert getattr(msg, field) == value


def assertNack(msg: Message, code: int, **kwargs) -> None:
    assertMsgEqual(msg, ack.Acknowledgement, code=code, **kwargs)


def _check(expect: Union[int, type, Message, Callable[[Message], None]], **kwargs) -> Callable[[Message], None]:
    if isinstance(expect, int):
        code = expect  # type: int
        return lambda msg: assertNack(msg, code, **kwargs)
    elif isinstance(expect, type) and issubclass(expect, Message):
        msg_class = expect  # type: Type[Message]
        return lambda msg: assertMsgEqual(msg, msg_class, **kwargs)
    elif isinstance(expect, Message):
        def assertEqual(a, b):
            assert a == b

        rep = expect  # type: Message
        return lambda msg: assertEqual(msg, rep)
    else:
        check = expect  # type: Callable[[Message], None]
        return check


async def assertReplyReq(socket, req: Message,
                   rep: Union[int, type, Message, Callable[[Message], None]], **kwargs) -> Message:
    check = _check(rep, **kwargs)

    await socket.send_msg(req)
    response = await socket.recv_msg()
    check(response)
    return response


async def assertReplyDealer(socket, req: Message,
                      rep: Union[int, type, Message, Callable[[Message], None]], **kwargs) -> Message:
    check = _check(rep, **kwargs)

    await socket.send_msg([], req)
    _, response = await socket.recv_msg()
    check(response)
    return response


@pytest.mark.asyncio
async def test_multipart(event_loop):
    async with connectSimulatorReq() as socket:
        await socket.send_msgs([analog.Request(0), digital.Request(0)])
        update = await socket.recv_msgs()
        assert update[0] == analog.Reply(0, 0)
        assert update[1] == digital.Reply(0, False)


@pytest.mark.asyncio
async def test_unsupported(event_loop):
    from hedgehog.server import handlers
    from hedgehog.server.handlers.hardware import HardwareHandler
    from hedgehog.server.handlers.process import ProcessHandler
    from hedgehog.server.hardware import HardwareAdapter
    adapter = HardwareAdapter()
    _handlers = handlers.to_dict(HardwareHandler(adapter), ProcessHandler(adapter))

    async with connectSimulatorReq(_handlers) as socket:
        await assertReplyReq(socket, io.Action(0, io.INPUT_PULLDOWN), ack.UNSUPPORTED_COMMAND)
        await assertReplyReq(socket, analog.Request(0), ack.UNSUPPORTED_COMMAND)
        await assertReplyReq(socket, digital.Request(0), ack.UNSUPPORTED_COMMAND)
        await assertReplyReq(socket, motor.Action(0, motor.POWER), ack.UNSUPPORTED_COMMAND)
        await assertReplyReq(socket, motor.StateRequest(0), ack.UNSUPPORTED_COMMAND)
        await assertReplyReq(socket, motor.SetPositionAction(0, 0), ack.UNSUPPORTED_COMMAND)
        await assertReplyReq(socket, servo.Action(0, True, 0), ack.UNSUPPORTED_COMMAND)


@pytest.mark.asyncio
async def test_io(event_loop):
    async with connectSimulatorDealer() as socket:
        # ### io.CommandRequest

        await assertReplyDealer(socket, io.CommandRequest(0), ack.FAILED_COMMAND)

        # ### io.Action

        await assertReplyDealer(socket, io.Action(0, io.INPUT_PULLDOWN), ack.Acknowledgement())

        # send an invalid command
        action = io.Action(0, 0)
        action.flags = io.OUTPUT | io.PULLDOWN
        await assertReplyDealer(socket, action, ack.INVALID_COMMAND)

        # ### io.CommandRequest

        await assertReplyDealer(socket, io.CommandRequest(0), io.CommandReply(0, io.INPUT_PULLDOWN))

        # # ### io.CommandSubscribe
        #
        # sub = Subscription()
        # sub.subscribe = False
        # sub.timeout = 10
        # await assertReplyDealer(socket, io.CommandSubscribe(0, sub), ack.FAILED_COMMAND)
        #
        # sub = Subscription()
        # sub.subscribe = True
        # sub.timeout = 10
        # await assertReplyDealer(socket, io.CommandSubscribe(0, sub), ack.Acknowledgement())
        #
        # # check immediate update
        # assert socket.poll(5) == zmq.POLLIN
        # _, response = socket.recv_msg()
        # assert response == io.CommandUpdate(0, io.INPUT_PULLDOWN, sub)
        #
        # sub = Subscription()
        # sub.subscribe = False
        # sub.timeout = 10
        # await assertReplyDealer(socket, io.CommandSubscribe(0, sub), ack.Acknowledgement())


# def test_command_subscription(self):
#     with connectSimulatorDealer() as socket:
#         sub = Subscription()
#         sub.subscribe = True
#         sub.timeout = 10
#
#         unsub = Subscription()
#         unsub.subscribe = False
#         unsub.timeout = 10
#
#         # original subscription
#         assertReplyDealer(socket, io.CommandSubscribe(0, sub), ack.Acknowledgement())
#
#         # check there is no update, even after a time
#         assert socket.poll(50) == 0
#
#         # send a first command to get an update
#         assertReplyDealer(socket, io.Action(0, io.INPUT_PULLDOWN), ack.Acknowledgement())
#
#         # check immediate update
#         assert socket.poll(5) == zmq.POLLIN
#         _, response = socket.recv_msg()
#         assert response == io.CommandUpdate(0, io.INPUT_PULLDOWN, sub)
#
#         # send another command that does not actually change the value
#         assertReplyDealer(socket, io.Action(0, io.INPUT_PULLDOWN), ack.Acknowledgement())
#
#         # check there is no update, even after a time
#         assert socket.poll(50) == 0
#
#         # change command value
#         assertReplyDealer(socket, io.Action(0, io.INPUT_PULLUP), ack.Acknowledgement())
#
#         # check immediate update (as time has passed)
#         assert socket.poll(5) == zmq.POLLIN
#         _, response = socket.recv_msg()
#         assert response == io.CommandUpdate(0, io.INPUT_PULLUP, sub)
#
#         # change command value
#         assertReplyDealer(socket, io.Action(0, io.INPUT_PULLDOWN), ack.Acknowledgement())
#
#         # check update is not immediately
#         assert socket.poll(5) == 0
#         _, response = socket.recv_msg()
#         assert response == io.CommandUpdate(0, io.INPUT_PULLDOWN, sub)
#
#         # add extra subscription
#         assertReplyDealer(socket, io.CommandSubscribe(0, sub), ack.Acknowledgement())
#
#         # check update is not immediately
#         assert socket.poll(5) == 0
#         _, response = socket.recv_msg()
#         assert response == io.CommandUpdate(0, io.INPUT_PULLDOWN, sub)
#
#         # cancel extra subscription
#         assertReplyDealer(socket, io.CommandSubscribe(0, unsub), ack.Acknowledgement())
#
#         # change command value
#         assertReplyDealer(socket, io.Action(0, io.INPUT_PULLUP), ack.Acknowledgement())
#
#         # check update is not immediately
#         assert socket.poll(5) == 0
#         _, response = socket.recv_msg()
#         assert response == io.CommandUpdate(0, io.INPUT_PULLUP, sub)
#
#         # cancel original subscription
#         assertReplyDealer(socket, io.CommandSubscribe(0, unsub), ack.Acknowledgement())
#
#
# def test_analog(self):
#     with connectSimulatorDealer() as socket:
#         # ### analog.Request
#
#         assertReplyDealer(socket, analog.Request(0), analog.Reply(0, 0))
#
#         # ### analog.Subscribe
#
#         sub = Subscription()
#         sub.subscribe = False
#         sub.timeout = 10
#         assertReplyDealer(socket, analog.Subscribe(0, sub), ack.FAILED_COMMAND)
#
#         sub = Subscription()
#         sub.subscribe = True
#         sub.timeout = 10
#         assertReplyDealer(socket, analog.Subscribe(0, sub), ack.Acknowledgement())
#
#         # check immediate update
#         assert socket.poll(5) == zmq.POLLIN
#         _, response = socket.recv_msg()
#         assert response == analog.Update(0, 0, sub)
#
#         sub = Subscription()
#         sub.subscribe = False
#         sub.timeout = 10
#         assertReplyDealer(socket, analog.Subscribe(0, sub), ack.Acknowledgement())
#
#
# def test_sensor_subscription(self):
#     with connectSimulatorDealer() as socket:
#         sub = Subscription()
#         sub.subscribe = True
#         sub.timeout = 10
#
#         unsub = Subscription()
#         unsub.subscribe = False
#         unsub.timeout = 10
#
#         # original subscription
#         assertReplyDealer(socket, analog.Subscribe(0, sub), ack.Acknowledgement())
#
#         # check immediate update
#         assert socket.poll(5) == zmq.POLLIN
#         _, response = socket.recv_msg()
#         assert response == analog.Update(0, 0, sub)
#
#         # check there is no update, even after a time
#         assert socket.poll(50) == 0
#
#         # add extra subscription
#         assertReplyDealer(socket, analog.Subscribe(0, sub), ack.Acknowledgement())
#
#         # check immediate update
#         # TODO update is not quite immediately with current implementation
#         assert socket.poll(15) == zmq.POLLIN
#         _, response = socket.recv_msg()
#         assert response == analog.Update(0, 0, sub)
#
#         # cancel extra subscription
#         assertReplyDealer(socket, analog.Subscribe(0, unsub), ack.Acknowledgement())
#
#         # cancel original subscription
#         assertReplyDealer(socket, analog.Subscribe(0, unsub), ack.Acknowledgement())
#
#
# def test_digital(self):
#     with connectSimulatorDealer() as socket:
#         # ### digital.Request
#
#         assertReplyDealer(socket, digital.Request(0), digital.Reply(0, False))
#
#         # ### digital.Subscribe
#
#         sub = Subscription()
#         sub.subscribe = False
#         sub.timeout = 10
#         assertReplyDealer(socket, digital.Subscribe(0, sub), ack.FAILED_COMMAND)
#
#         sub = Subscription()
#         sub.subscribe = True
#         sub.timeout = 10
#         assertReplyDealer(socket, digital.Subscribe(0, sub), ack.Acknowledgement())
#
#         # check immediate update
#         assert socket.poll(5) == zmq.POLLIN
#         _, response = socket.recv_msg()
#         assert response == digital.Update(0, False, sub)
#
#         sub = Subscription()
#         sub.subscribe = False
#         sub.timeout = 10
#         assertReplyDealer(socket, digital.Subscribe(0, sub), ack.Acknowledgement())
#
#
# def test_motor(self):
#     with connectSimulatorDealer() as socket:
#         # ### motor.CommandRequest
#
#         assertReplyDealer(socket, motor.CommandRequest(0), ack.FAILED_COMMAND)
#
#         # ### motor.Action
#
#         assertReplyDealer(socket, motor.Action(0, motor.POWER), ack.Acknowledgement())
#
#         # send an invalid command
#         action = motor.Action(0, motor.BRAKE)
#         action.relative = 100
#         assertReplyDealer(socket, action, ack.INVALID_COMMAND)
#
#         # ### motor.CommandRequest
#
#         assertReplyDealer(socket, motor.CommandRequest(0), motor.CommandReply(0, motor.POWER, 0))
#
#         # ### motor.StateRequest
#
#         assertReplyDealer(socket, motor.StateRequest(0), motor.StateReply(0, 0, 0))
#
#         # ### motor.SetPositionAction
#
#         assertReplyDealer(socket, motor.SetPositionAction(0, 0), ack.Acknowledgement())
#
#         # ### motor.CommandSubscribe
#
#         sub = Subscription()
#         sub.subscribe = False
#         sub.timeout = 10
#         assertReplyDealer(socket, motor.CommandSubscribe(0, sub), ack.FAILED_COMMAND)
#
#         sub = Subscription()
#         sub.subscribe = True
#         sub.timeout = 10
#         assertReplyDealer(socket, motor.CommandSubscribe(0, sub), ack.Acknowledgement())
#
#         # check immediate update
#         assert socket.poll(5) == zmq.POLLIN
#         _, response = socket.recv_msg()
#         assert response == motor.CommandUpdate(0, motor.POWER, 0, sub)
#
#         sub = Subscription()
#         sub.subscribe = False
#         sub.timeout = 10
#         assertReplyDealer(socket, motor.CommandSubscribe(0, sub), ack.Acknowledgement())
#
#         # ### motor.StateSubscribe
#
#         sub = Subscription()
#         sub.subscribe = False
#         sub.timeout = 10
#         assertReplyDealer(socket, motor.StateSubscribe(0, sub), ack.FAILED_COMMAND)
#
#         sub = Subscription()
#         sub.subscribe = True
#         sub.timeout = 10
#         assertReplyDealer(socket, motor.StateSubscribe(0, sub), ack.Acknowledgement())
#
#         # check immediate update
#         assert socket.poll(5) == zmq.POLLIN
#         _, response = socket.recv_msg()
#         assert response == motor.StateUpdate(0, 0, 0, sub)
#
#         sub = Subscription()
#         sub.subscribe = False
#         sub.timeout = 10
#         assertReplyDealer(socket, motor.StateSubscribe(0, sub), ack.Acknowledgement())
#
#
# def test_servo(self):
#     with connectSimulatorDealer() as socket:
#         # ### servo.CommandRequest
#
#         assertReplyDealer(socket, servo.CommandRequest(0), ack.FAILED_COMMAND)
#
#         # ### servo.Action
#
#         assertReplyDealer(socket, servo.Action(0, True, 0), ack.Acknowledgement())
#
#         # ### servo.CommandRequest
#
#         assertReplyDealer(socket, servo.CommandRequest(0), servo.CommandReply(0, True, 0))
#
#         # ### servo.CommandSubscribe
#
#         sub = Subscription()
#         sub.subscribe = False
#         sub.timeout = 10
#         assertReplyDealer(socket, servo.CommandSubscribe(0, sub), ack.FAILED_COMMAND)
#
#         sub = Subscription()
#         sub.subscribe = True
#         sub.timeout = 10
#         assertReplyDealer(socket, servo.CommandSubscribe(0, sub), ack.Acknowledgement())
#
#         # check immediate update
#         assert socket.poll(5) == zmq.POLLIN
#         _, response = socket.recv_msg()
#         assert response == servo.CommandUpdate(0, True, 0, sub)
#
#         sub = Subscription()
#         sub.subscribe = False
#         sub.timeout = 10
#         assertReplyDealer(socket, servo.CommandSubscribe(0, sub), ack.Acknowledgement())
#
#
# def handle_streams(self) -> Callable[[process.StreamUpdate], Dict[int, bytes]]:
#     def handler():
#         outputs = {
#             process.STDOUT: [],
#             process.STDERR: [],
#         }  # type: Dict[int, List[bytes]]
#
#         open = len(outputs)
#         while open > 0:
#             msg = yield
#             outputs[msg.fileno].append(msg.chunk)
#             if msg.chunk == b'':
#                 open -= 1
#
#         return {fileno: b''.join(chunks) for fileno, chunks in outputs.items()}
#
#     gen = handler()
#     gen.send(None)
#
#     def send(msg: process.StreamUpdate) -> Dict[int, bytes]:
#         try:
#             gen.send(msg)
#             return None
#         except StopIteration as stop:
#             return stop.value
#
#     return send
#
#
# def test_process_echo(self):
#     with connectSimulatorDealer() as socket:
#         response = assertReplyDealer(socket, process.ExecuteAction('echo', 'asdf'),
#                                           process.ExecuteReply)  # type: process.ExecuteReply
#         pid = response.pid
#
#         stream_handler = self.handle_streams()
#         output = None
#         while output is None:
#             _, msg = socket.recv_msg()  # type: Tuple[Any, process.StreamUpdate]
#             assertMsgEqual(msg, process.StreamUpdate, pid=pid)
#             output = stream_handler(msg)
#
#         assertReplyDealer(socket, process.StreamAction(pid, process.STDIN, b''), ack.Acknowledgement())
#
#         _, msg = socket.recv_msg()
#         assert msg == process.ExitUpdate(pid, 0)
#
#         assert output[process.STDOUT] == b'asdf\n'
#         assert output[process.STDERR] == b''
#
#
# def test_process_cat(self):
#     with connectSimulatorDealer() as socket:
#         response = assertReplyDealer(socket, process.ExecuteAction('cat'),
#                                           process.ExecuteReply)  # type: process.ExecuteReply
#         pid = response.pid
#
#         assertReplyDealer(socket, process.StreamAction(pid, process.STDIN, b'asdf'), ack.Acknowledgement())
#         assertReplyDealer(socket, process.StreamAction(pid, process.STDIN, b''), ack.Acknowledgement())
#
#         stream_handler = self.handle_streams()
#         output = None
#         while output is None:
#             _, msg = socket.recv_msg()  # type: Tuple[Any, process.StreamUpdate]
#             assertMsgEqual(msg, process.StreamUpdate, pid=pid)
#             output = stream_handler(msg)
#
#         _, msg = socket.recv_msg()
#         assert msg == process.ExitUpdate(pid, 0)
#
#         assert output[process.STDOUT] == b'asdf'
#         assert output[process.STDERR] == b''
#
#
# def test_process_pwd(self):
#     with connectSimulatorDealer() as socket:
#         response = assertReplyDealer(socket, process.ExecuteAction('pwd', working_dir='/'),
#                                           process.ExecuteReply)  # type: process.ExecuteReply
#         pid = response.pid
#
#         stream_handler = self.handle_streams()
#         output = None
#         while output is None:
#             _, msg = socket.recv_msg()  # type: Tuple[Any, process.StreamUpdate]
#             assertMsgEqual(msg, process.StreamUpdate, pid=pid)
#             output = stream_handler(msg)
#
#         assertReplyDealer(socket, process.StreamAction(pid, process.STDIN, b''), ack.Acknowledgement())
#
#         _, msg = socket.recv_msg()
#         assert msg == process.ExitUpdate(pid, 0)
#
#         assert output[process.STDOUT] == b'/\n'
#         assert output[process.STDERR] == b''
#
#
# def test_process_sleep(self):
#     with connectSimulatorDealer() as socket:
#         response = assertReplyDealer(socket, process.ExecuteAction('sleep', '1'),
#                                           process.ExecuteReply)  # type: process.ExecuteReply
#         pid = response.pid
#
#         assertReplyDealer(socket, process.StreamAction(pid, process.STDIN, b''), ack.Acknowledgement())
#         assertReplyDealer(socket, process.SignalAction(pid, signal.SIGINT), ack.Acknowledgement())
#
#         stream_handler = self.handle_streams()
#         output = None
#         while output is None:
#             _, msg = socket.recv_msg()  # type: Tuple[Any, process.StreamUpdate]
#             assertMsgEqual(msg, process.StreamUpdate, pid=pid)
#             output = stream_handler(msg)
#
#         _, msg = socket.recv_msg()
#         assert msg == process.ExitUpdate(pid, -signal.SIGINT)
#
#
# def collect_outputs(proc):
#     output = {process.STDOUT: [], process.STDERR: []}  # type: Dict[int, List[bytes]]
#
#     msg = proc.read()
#     while msg is not None:
#         fileno, msg = msg
#         if msg != b'':
#             output[fileno].append(msg)
#         msg = proc.read()
#
#     return proc.returncode, b''.join(output[process.STDOUT]), b''.join(output[process.STDERR])
#
#
# class TestProcess(object):
#     def test_cat(self):
#         proc = Process('cat')
#
#         proc.write(process.STDIN, b'as ')
#         time.sleep(0.1)
#         proc.write(process.STDIN, b'df')
#         proc.write(process.STDIN)
#
#         status, out, err = collect_outputs(proc)
#         assert status == 0
#         assert out == b'as df'
#         assert err == b''
#
#     def test_echo(self):
#         proc = Process('echo', 'as', 'df')
#
#         proc.write(process.STDIN)
#
#         status, out, err = collect_outputs(proc)
#         assert status == 0
#         assert out == b'as df\n'
#         assert err == b''
#
#     def test_pwd(self):
#         proc = Process('pwd', cwd='/')
#
#         proc.write(process.STDIN)
#
#         status, out, err = collect_outputs(proc)
#         assert status == 0
#         assert out == b'/\n'
#         assert err == b''
#
#     def test_signal_sleep(self):
#         proc = Process('sleep', '1')
#
#         proc.write(process.STDIN)
#
#         proc.send_signal(signal.SIGINT)
#
#         status, out, err = collect_outputs(proc)
#         assert status == -2
#         assert out == b''
#         assert err == b''
