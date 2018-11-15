from typing import Any, Callable, Dict, List, Tuple, Type, Union

import pytest
import pytest_trio
from hedgehog.utils.test_utils import check_caplog, assertImmediate, assertPassed

import logging
import signal
import trio
import trio_asyncio
import zmq.asyncio
from contextlib import asynccontextmanager

from hedgehog.protocol import ClientSide
from hedgehog.protocol.errors import FailedCommandError
from hedgehog.protocol.messages import Message, ack, io, analog, digital, motor, servo, process
from hedgehog.protocol.proto.subscription_pb2 import Subscription
from hedgehog.protocol.zmq.trio import ReqSocket, DealerRouterSocket
from hedgehog.server import handlers, HedgehogServer
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware import HardwareAdapter
from hedgehog.server.hardware.mocked import MockedHardwareAdapter
from hedgehog.utils.zmq import trio as zmq_trio


# Pytest fixtures
check_caplog
pytestmark = pytest.mark.usefixtures('check_caplog')


@pytest.fixture
def hardware_adapter():
    return MockedHardwareAdapter()


@pytest.fixture
def handler_dict(hardware_adapter: HardwareAdapter) -> handlers.HandlerCallbackDict:
    return handlers.merge(HardwareHandler(hardware_adapter), ProcessHandler(hardware_adapter))


@pytest.fixture
def trio_aio_loop():
    loop = None

    @asynccontextmanager
    async def open_loop():
        nonlocal loop
        if loop is None:
            async with trio_asyncio.open_loop() as loop:
                yield loop
        else:
            yield loop

    return open_loop


@pytest.fixture
def zmq_trio_ctx():
    from hedgehog.utils.zmq.trio import Context

    ctx = None

    @asynccontextmanager
    async def open_context():
        nonlocal ctx
        if ctx is None:
            with Context() as ctx:
                yield ctx
        else:
            yield ctx

    return open_context


@pytest.fixture
def hedgehog_server(trio_aio_loop, zmq_trio_ctx,
                    hardware_adapter: HardwareAdapter, handler_dict: handlers.HandlerCallbackDict):
    @asynccontextmanager
    async def start_server(endpoint: str='inproc://controller', *,
                           handler_dict: handlers.HandlerCallbackDict=handler_dict):
        async with trio_aio_loop(), zmq_trio_ctx() as ctx, hardware_adapter, trio.open_nursery() as nursery:
            server = HedgehogServer(ctx, endpoint, handler_dict)
            await nursery.start(server.run)

            yield server

            # if an exception leads to this line being skipped, the nursery kills the server anyway
            server.stop()

    return start_server


@pytest.fixture
async def client_req(trio_aio_loop, zmq_trio_ctx):
    @asynccontextmanager
    async def connect(endpoint: str='inproc://controller'):
        async with trio_aio_loop(), zmq_trio_ctx() as ctx:
            with ReqSocket(ctx, zmq.REQ, side=ClientSide) as socket:
                socket.connect(endpoint)
                yield socket

    return connect


@pytest.fixture
async def client_dealer(trio_aio_loop, zmq_trio_ctx):
    @asynccontextmanager
    async def connect(endpoint: str='inproc://controller'):
        async with trio_aio_loop(), zmq_trio_ctx() as ctx:
            with DealerRouterSocket(ctx, zmq.DEALER, side=ClientSide) as socket:
                socket.connect(endpoint)
                yield socket

    return connect


@pytest.fixture
async def conn_req(hedgehog_server, client_req, handler_dict: handlers.HandlerCallbackDict):
    @asynccontextmanager
    async def connect(endpoint: str='inproc://controller', *,
                      handler_dict: handlers.HandlerCallbackDict=handler_dict):
        async with hedgehog_server(endpoint=endpoint, handler_dict=handler_dict), \
                   client_req(endpoint=endpoint) as socket:
            yield socket

    return connect


@pytest.fixture
async def conn_dealer(hedgehog_server, client_dealer, handler_dict: handlers.HandlerCallbackDict):
    @asynccontextmanager
    async def connect(endpoint: str='inproc://controller', *,
                      handler_dict: handlers.HandlerCallbackDict=handler_dict):
        async with hedgehog_server(endpoint=endpoint, handler_dict=handler_dict), \
                   client_dealer(endpoint=endpoint) as socket:
            yield socket

    return connect


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

    with assertImmediate():
        await socket.send_msg(req)
        response = await socket.recv_msg()
    check(response)
    return response


async def assertReplyDealer(socket, req: Message,
                      rep: Union[int, type, Message, Callable[[Message], None]], **kwargs) -> Message:
    check = _check(rep, **kwargs)

    with assertImmediate():
        await socket.send_msg([], req)
        _, response = await socket.recv_msg()
    check(response)
    return response


@pytest.mark.trio
async def test_server_faulty_task(caplog, check_caplog, conn_req, autojump_clock):
    async def handler_callback(server, ident, msg):
        async def task(*, task_status=trio.TASK_STATUS_IGNORED):
            task_status.started()
            raise Exception

        await server.add_task(task)
        return ack.Acknowledgement()

    async with conn_req(handler_dict={io.Action: handler_callback}) as socket:
        await assertReplyReq(socket, io.Action(0, io.INPUT_FLOATING), ack.OK)
        await trio.sleep(0.1)

    records = [record for record in caplog.records if record.levelno >= logging.WARNING]
    assert len(records) == 1 and "Task raised an exception" in records[0].message
    check_caplog.expected.update(records)


@pytest.mark.trio
async def test_server_slow_job(caplog, check_caplog, conn_req, autojump_clock):
    async def handler_callback(server, ident, msg):
        async def task(*, task_status=trio.TASK_STATUS_IGNORED):
            task_status.started()
            async with server.job():
                await trio.sleep(0.2)

        await server.add_task(task)
        return ack.Acknowledgement()

    async with conn_req(handler_dict={io.Action: handler_callback}) as socket:
        await assertReplyReq(socket, io.Action(0, io.INPUT_FLOATING), ack.OK)
        await trio.sleep(0.3)

    records = [record for record in caplog.records if record.levelno >= logging.WARNING]
    assert len(records) == 2 \
           and "Long running job on server loop" in records[0].message \
           and "Long running job finished after 200.0 ms" in records[1].message
    check_caplog.expected.update(records)


@pytest.mark.trio
async def test_server_very_slow_job(caplog, check_caplog, conn_req, autojump_clock):
    async def handler_callback(server, ident, msg):
        async def task(*, task_status=trio.TASK_STATUS_IGNORED):
            task_status.started()
            async with server.job():
                await trio.sleep(10.1)

        await server.add_task(task)
        return ack.Acknowledgement()

    async with conn_req(handler_dict={io.Action: handler_callback}) as socket:
        await assertReplyReq(socket, io.Action(0, io.INPUT_FLOATING), ack.OK)
        await trio.sleep(10.2)

    records = [record for record in caplog.records if record.levelno >= logging.WARNING]
    assert len(records) == 3 \
           and "Long running job on server loop" in records[0].message \
           and "Long running job cancelled after 10000.0 ms" in records[1].message \
           and "Task raised an exception" in records[2].message
    check_caplog.expected.update(records)


@pytest.mark.trio
async def test_server_cancel_in_job(conn_req, autojump_clock):
    async def handler_callback(server, ident, msg):
        async def task(*, task_status=trio.TASK_STATUS_IGNORED):
            task_status.started()
            async with server.job():
                await trio.sleep(0.1)

        await server.add_task(task)
        return ack.Acknowledgement()

    async with conn_req(handler_dict={io.Action: handler_callback}) as socket:
        await assertReplyReq(socket, io.Action(0, io.INPUT_FLOATING), ack.OK)
        await trio.sleep(0.05)


@pytest.mark.trio
async def test_server_no_handler(conn_req, autojump_clock):
    async with conn_req(handler_dict={}) as socket:
        await assertReplyReq(socket, io.Action(0, io.INPUT_FLOATING), ack.UNSUPPORTED_COMMAND)


@pytest.mark.trio
async def test_server_faulty_handler(caplog, check_caplog, conn_req, autojump_clock):
    async def handler_callback(server, ident, msg):
        raise Exception

    async with conn_req(handler_dict={io.Action: handler_callback}) as socket:
        await assertReplyReq(socket, io.Action(0, io.INPUT_FLOATING), ack.FAILED_COMMAND)

    records = [record for record in caplog.records if record.levelno >= logging.WARNING]
    assert len(records) == 1 and "Uncaught exception in command handler" in records[0].message
    check_caplog.expected.update(records)


@pytest.mark.trio
async def test_server_failing_command(conn_req, autojump_clock):
    async def handler_callback(server, ident, msg):
        raise FailedCommandError

    async with conn_req(handler_dict={io.Action: handler_callback}) as socket:
        await assertReplyReq(socket, io.Action(0, io.INPUT_FLOATING), ack.FAILED_COMMAND)
        await trio.sleep(0.05)


@pytest.mark.trio
async def test_server_send_async(conn_dealer, autojump_clock):
    async def handler_callback(server, ident, msg):
        async def task(*, task_status=trio.TASK_STATUS_IGNORED):
            task_status.started()
            await trio.sleep(0.1)
            async with server.job():
                await server.send_async(ident, io.CommandUpdate(0, io.INPUT_FLOATING, Subscription()))

        await server.add_task(task)
        return ack.Acknowledgement()

    async with conn_dealer(handler_dict={io.Action: handler_callback}) as socket:
        await assertReplyDealer(socket, io.Action(0, io.INPUT_FLOATING), ack.OK)

        _, update = await socket.recv_msg()
        assertMsgEqual(update, io.CommandUpdate, port=0, flags=io.INPUT_FLOATING)


@pytest.mark.asyncio
async def test_multipart(conn_req):
    with assertImmediate():
        await conn_req.send_msgs([analog.Request(0), digital.Request(0)])
        update = await conn_req.recv_msgs()
    assert update[0] == analog.Reply(0, 0)
    assert update[1] == digital.Reply(0, False)


@pytest.mark.asyncio
@pytest.mark.parametrize('hardware_adapter', [HardwareAdapter()])
async def test_unsupported(conn_req):
    await assertReplyReq(conn_req, io.Action(0, io.INPUT_PULLDOWN), ack.UNSUPPORTED_COMMAND)
    await assertReplyReq(conn_req, analog.Request(0), ack.UNSUPPORTED_COMMAND)
    await assertReplyReq(conn_req, digital.Request(0), ack.UNSUPPORTED_COMMAND)
    await assertReplyReq(conn_req, motor.Action(0, motor.POWER), ack.UNSUPPORTED_COMMAND)
    await assertReplyReq(conn_req, motor.StateRequest(0), ack.UNSUPPORTED_COMMAND)
    await assertReplyReq(conn_req, motor.SetPositionAction(0, 0), ack.UNSUPPORTED_COMMAND)
    await assertReplyReq(conn_req, servo.Action(0, True, 0), ack.UNSUPPORTED_COMMAND)


@pytest.mark.asyncio
async def test_io(conn_dealer):
    # ### io.CommandRequest

    await assertReplyDealer(conn_dealer, io.CommandRequest(0), ack.FAILED_COMMAND)

    # ### io.Action

    await assertReplyDealer(conn_dealer, io.Action(0, io.INPUT_PULLDOWN), ack.Acknowledgement())

    # send an invalid command
    action = io.Action(0, 0)
    object.__setattr__(action, 'flags', io.OUTPUT | io.PULLDOWN)
    await assertReplyDealer(conn_dealer, action, ack.INVALID_COMMAND)

    # ### io.CommandRequest

    await assertReplyDealer(conn_dealer, io.CommandRequest(0), io.CommandReply(0, io.INPUT_PULLDOWN))

    # ### io.CommandSubscribe

    sub = Subscription()
    sub.subscribe = False
    await assertReplyDealer(conn_dealer, io.CommandSubscribe(0, sub), ack.FAILED_COMMAND)

    sub = Subscription()
    sub.subscribe = True
    await assertReplyDealer(conn_dealer, io.CommandSubscribe(0, sub), ack.Acknowledgement())

    await assertTimeout(conn_dealer.recv_multipart(), 1)

    with assertImmediate():
        await assertReplyDealer(conn_dealer, io.Action(0, io.INPUT_PULLDOWN), ack.Acknowledgement())

        _, response = await conn_dealer.recv_msg()
        assert response == io.CommandUpdate(0, io.INPUT_PULLDOWN, sub)

    with assertImmediate():
        await assertReplyDealer(conn_dealer, io.Action(0, io.INPUT_PULLUP), ack.Acknowledgement())

        _, response = await conn_dealer.recv_msg()
        assert response == io.CommandUpdate(0, io.INPUT_PULLUP, sub)

    sub = Subscription()
    sub.subscribe = False
    await assertReplyDealer(conn_dealer, io.CommandSubscribe(0, sub), ack.Acknowledgement())


@pytest.mark.asyncio
async def test_command_subscription(conn_dealer):
    sub = Subscription()
    sub.subscribe = True
    sub.timeout = 1000

    unsub = Subscription()
    unsub.subscribe = False
    unsub.timeout = 1000

    # original subscription
    await assertReplyDealer(conn_dealer, io.CommandSubscribe(0, sub), ack.Acknowledgement())

    # check there is no update, even after a time
    await assertTimeout(conn_dealer.recv_multipart(), 2)

    # check immediate update
    with assertImmediate():
        # send a first command to get an update
        await assertReplyDealer(conn_dealer, io.Action(0, io.INPUT_PULLDOWN), ack.Acknowledgement())

        _, response = await conn_dealer.recv_msg()
        assert response == io.CommandUpdate(0, io.INPUT_PULLDOWN, sub)

    # send another command that does not actually change the value
    await assertReplyDealer(conn_dealer, io.Action(0, io.INPUT_PULLDOWN), ack.Acknowledgement())

    # check there is no update, even after a time
    await assertTimeout(conn_dealer.recv_multipart(), 2)

    # check immediate update (as time has passed)
    with assertImmediate():
        # change command value
        await assertReplyDealer(conn_dealer, io.Action(0, io.INPUT_PULLUP), ack.Acknowledgement())

        _, response = await conn_dealer.recv_msg()
        assert response == io.CommandUpdate(0, io.INPUT_PULLUP, sub)

    # check update is not immediately
    with assertPassed(1):
        # change command value
        await assertReplyDealer(conn_dealer, io.Action(0, io.INPUT_PULLDOWN), ack.Acknowledgement())

        _, response = await conn_dealer.recv_msg()
        assert response == io.CommandUpdate(0, io.INPUT_PULLDOWN, sub)

    # FIXME no immediate update
    # # check update is not immediately
    # with assertPassed(1):
    #     # add extra subscription
    #     await assertReplyDealer(socket, io.CommandSubscribe(0, sub), ack.Acknowledgement())
    #
    #     _, response = await socket.recv_msg()
    #     assert response == io.CommandUpdate(0, io.INPUT_PULLDOWN, sub)
    #
    # # cancel extra subscription
    # await assertReplyDealer(socket, io.CommandSubscribe(0, unsub), ack.Acknowledgement())

    # check update is not immediately
    with assertPassed(1):
        # change command value
        await assertReplyDealer(conn_dealer, io.Action(0, io.INPUT_PULLUP), ack.Acknowledgement())

        _, response = await conn_dealer.recv_msg()
        assert response == io.CommandUpdate(0, io.INPUT_PULLUP, sub)

    # cancel original subscription
    await assertReplyDealer(conn_dealer, io.CommandSubscribe(0, unsub), ack.Acknowledgement())

    # change command value
    await assertReplyDealer(conn_dealer, io.Action(0, io.INPUT_PULLDOWN), ack.Acknowledgement())

    # check there is no update, even after a time
    await assertTimeout(conn_dealer.recv_multipart(), 2)


@pytest.mark.asyncio
async def test_analog(conn_dealer):
    # ### analog.Request

    await assertReplyDealer(conn_dealer, analog.Request(0), analog.Reply(0, 0))

    # ### analog.Subscribe

    sub = Subscription()
    sub.subscribe = False
    sub.timeout = 1000
    await assertReplyDealer(conn_dealer, analog.Subscribe(0, sub), ack.FAILED_COMMAND)

    with assertImmediate():
        sub = Subscription()
        sub.subscribe = True
        sub.timeout = 1000
        await assertReplyDealer(conn_dealer, analog.Subscribe(0, sub), ack.Acknowledgement())

        _, response = await conn_dealer.recv_msg()
        assert response == analog.Update(0, 0, sub)

    sub = Subscription()
    sub.subscribe = False
    sub.timeout = 1000
    await assertReplyDealer(conn_dealer, analog.Subscribe(0, sub), ack.Acknowledgement())


@pytest.mark.asyncio
async def test_sensor_subscription(conn_dealer, hardware_adapter):
    hardware_adapter.set_analog(0, 0.5, 100)
    hardware_adapter.set_analog(0, 3, 0)

    sub = Subscription()
    sub.subscribe = True
    sub.timeout = 1000

    unsub = Subscription()
    unsub.subscribe = False
    unsub.timeout = 1000

    # check immediate update
    with assertImmediate():
        # original subscription
        await assertReplyDealer(conn_dealer, analog.Subscribe(0, sub), ack.Acknowledgement())

        _, response = await conn_dealer.recv_msg()
        assert response == analog.Update(0, 0, sub)

    # check the next update comes after one second, even though the change occurs earlier
    with assertPassed(1):
        _, response = await conn_dealer.recv_msg()
        assert response == analog.Update(0, 100, sub)

    # check the next update comes after two seconds, as the value didn't change earlier
    with assertPassed(2):
        _, response = await conn_dealer.recv_msg()
        assert response == analog.Update(0, 0, sub)

    # FIXME no update at all
    # # add extra subscription
    # await assertReplyDealer(socket, analog.Subscribe(0, sub), ack.Acknowledgement())
    #
    # # check immediate update
    # _, response = await socket.recv_msg()
    # assert response == analog.Update(0, 0, sub)
    #
    # # cancel extra subscription
    # await assertReplyDealer(socket, analog.Subscribe(0, unsub), ack.Acknowledgement())

    # cancel original subscription
    await assertReplyDealer(conn_dealer, analog.Subscribe(0, unsub), ack.Acknowledgement())

    # check there is no update, even after a time
    await assertTimeout(conn_dealer.recv_multipart(), 2)


@pytest.mark.asyncio
async def test_digital(conn_dealer):
    # ### digital.Request

    await assertReplyDealer(conn_dealer, digital.Request(0), digital.Reply(0, False))

    # ### digital.Subscribe

    sub = Subscription()
    sub.subscribe = False
    sub.timeout = 1000
    await assertReplyDealer(conn_dealer, digital.Subscribe(0, sub), ack.FAILED_COMMAND)

    with assertImmediate():
        sub = Subscription()
        sub.subscribe = True
        sub.timeout = 1000
        await assertReplyDealer(conn_dealer, digital.Subscribe(0, sub), ack.Acknowledgement())

        _, response = await conn_dealer.recv_msg()
        assert response == digital.Update(0, False, sub)

    sub = Subscription()
    sub.subscribe = False
    sub.timeout = 1000
    await assertReplyDealer(conn_dealer, digital.Subscribe(0, sub), ack.Acknowledgement())


@pytest.mark.asyncio
async def test_motor(conn_dealer):
    # ### motor.CommandRequest

    await assertReplyDealer(conn_dealer, motor.CommandRequest(0), ack.FAILED_COMMAND)

    # ### motor.Action

    await assertReplyDealer(conn_dealer, motor.Action(0, motor.POWER), ack.Acknowledgement())

    # send an invalid command
    action = motor.Action(0, motor.BRAKE)
    object.__setattr__(action, 'relative', 100)
    await assertReplyDealer(conn_dealer, action, ack.INVALID_COMMAND)

    # ### motor.CommandRequest

    await assertReplyDealer(conn_dealer, motor.CommandRequest(0), motor.CommandReply(0, motor.POWER, 0))

    # ### motor.StateRequest

    await assertReplyDealer(conn_dealer, motor.StateRequest(0), motor.StateReply(0, 0, 0))

    # ### motor.SetPositionAction

    await assertReplyDealer(conn_dealer, motor.SetPositionAction(0, 0), ack.Acknowledgement())

    # ### motor.CommandSubscribe

    sub = Subscription()
    sub.subscribe = False
    sub.timeout = 1000
    await assertReplyDealer(conn_dealer, motor.CommandSubscribe(0, sub), ack.FAILED_COMMAND)

    # FIXME no immediate update
    # with assertImmediate():
    #     sub = Subscription()
    #     sub.subscribe = True
    #     sub.timeout = 1000
    #     await assertReplyDealer(socket, motor.CommandSubscribe(0, sub), ack.Acknowledgement())
    #
    #     _, response = await socket.recv_msg()
    #     assert response == motor.CommandUpdate(0, motor.POWER, 0, sub)
    #
    # sub = Subscription()
    # sub.subscribe = False
    # sub.timeout = 1000
    # await assertReplyDealer(socket, motor.CommandSubscribe(0, sub), ack.Acknowledgement())
    #
    # ### motor.StateSubscribe

    # FIXME
    # sub = Subscription()
    # sub.subscribe = False
    # sub.timeout = 1000
    # await assertReplyDealer(socket, motor.StateSubscribe(0, sub), ack.FAILED_COMMAND)
    #
    # with assertImmediate():
    #     sub = Subscription()
    #     sub.subscribe = True
    #     sub.timeout = 1000
    #     await assertReplyDealer(socket, motor.StateSubscribe(0, sub), ack.Acknowledgement())
    #
    #     _, response = await socket.recv_msg()
    #     assert response == motor.StateUpdate(0, 0, 0, sub)
    #
    # sub = Subscription()
    # sub.subscribe = False
    # sub.timeout = 1000
    # await assertReplyDealer(socket, motor.StateSubscribe(0, sub), ack.Acknowledgement())


@pytest.mark.asyncio
async def test_servo(conn_dealer):
    # ### servo.CommandRequest

    await assertReplyDealer(conn_dealer, servo.CommandRequest(0), ack.FAILED_COMMAND)

    # ### servo.Action

    await assertReplyDealer(conn_dealer, servo.Action(0, True, 0), ack.Acknowledgement())

    # ### servo.CommandRequest

    await assertReplyDealer(conn_dealer, servo.CommandRequest(0), servo.CommandReply(0, True, 0))

    # ### servo.CommandSubscribe

    sub = Subscription()
    sub.subscribe = False
    sub.timeout = 10
    await assertReplyDealer(conn_dealer, servo.CommandSubscribe(0, sub), ack.FAILED_COMMAND)

    # FIXME no immediate update
    # with assertImmediate():
    #     sub = Subscription()
    #     sub.subscribe = True
    #     sub.timeout = 10
    #     await assertReplyDealer(socket, servo.CommandSubscribe(0, sub), ack.Acknowledgement())
    #
    #     _, response = await socket.recv_msg()
    #     assert response == servo.CommandUpdate(0, True, 0, sub)
    #
    # sub = Subscription()
    # sub.subscribe = False
    # sub.timeout = 10
    # await assertReplyDealer(socket, servo.CommandSubscribe(0, sub), ack.Acknowledgement())


def handle_streams() -> Callable[[process.StreamUpdate], Dict[int, bytes]]:
    outputs = {
        process.STDOUT: [],
        process.STDERR: [],
    }  # type: Dict[int, List[bytes]]

    open = len(outputs)

    def send(msg: process.StreamUpdate):
        nonlocal outputs, open

        outputs[msg.fileno].append(msg.chunk)
        if msg.chunk == b'':
            open -= 1
        if open > 0:
            return None
        return {fileno: b''.join(chunks) for fileno, chunks in outputs.items()}

    return send


@pytest.mark.asyncio
async def test_process_echo(conn_dealer):
        response = await assertReplyDealer(conn_dealer, process.ExecuteAction('echo', 'asdf'),
                                           process.ExecuteReply)  # type: process.ExecuteReply
        pid = response.pid

        stream_handler = handle_streams()
        output = None

        async def handle():
            nonlocal output
            _, msg = await conn_dealer.recv_msg()  # type: Tuple[Any, process.StreamUpdate]
            assertMsgEqual(msg, process.StreamUpdate, pid=pid)
            output = stream_handler(msg)

        while output is None:
            await handle()

        _, msg = await conn_dealer.recv_msg()
        assert msg == process.ExitUpdate(pid, 0)

        assert output[process.STDOUT] == b'asdf\n'
        assert output[process.STDERR] == b''


@pytest.mark.asyncio
async def test_process_cat(conn_dealer):
    response = await assertReplyDealer(conn_dealer, process.ExecuteAction('cat'),
                                       process.ExecuteReply)  # type: process.ExecuteReply
    pid = response.pid

    stream_handler = handle_streams()
    output = None

    async def handle():
        nonlocal output
        _, msg = await conn_dealer.recv_msg()  # type: Tuple[Any, process.StreamUpdate]
        assertMsgEqual(msg, process.StreamUpdate, pid=pid)
        output = stream_handler(msg)

    await assertReplyDealer(conn_dealer, process.StreamAction(pid, process.STDIN, b'asdf'), ack.Acknowledgement())
    await handle()
    await assertReplyDealer(conn_dealer, process.StreamAction(pid, process.STDIN, b''), ack.Acknowledgement())

    while output is None:
        await handle()

    _, msg = await conn_dealer.recv_msg()
    assert msg == process.ExitUpdate(pid, 0)

    assert output[process.STDOUT] == b'asdf'
    assert output[process.STDERR] == b''


@pytest.mark.asyncio
async def test_process_pwd(conn_dealer):
    response = await assertReplyDealer(conn_dealer, process.ExecuteAction('pwd', working_dir='/'),
                                       process.ExecuteReply)  # type: process.ExecuteReply
    pid = response.pid

    stream_handler = handle_streams()
    output = None

    async def handle():
        nonlocal output
        _, msg = await conn_dealer.recv_msg()  # type: Tuple[Any, process.StreamUpdate]
        assertMsgEqual(msg, process.StreamUpdate, pid=pid)
        output = stream_handler(msg)

    while output is None:
        await handle()

    _, msg = await conn_dealer.recv_msg()
    assert msg == process.ExitUpdate(pid, 0)

    assert output[process.STDOUT] == b'/\n'
    assert output[process.STDERR] == b''


@pytest.mark.asyncio
async def test_process_sleep(conn_dealer):
    response = await assertReplyDealer(conn_dealer, process.ExecuteAction('sleep', '1'),
                                       process.ExecuteReply)  # type: process.ExecuteReply
    pid = response.pid

    stream_handler = handle_streams()
    output = None

    async def handle():
        nonlocal output
        _, msg = await conn_dealer.recv_msg()  # type: Tuple[Any, process.StreamUpdate]
        assertMsgEqual(msg, process.StreamUpdate, pid=pid)
        output = stream_handler(msg)

    await assertReplyDealer(conn_dealer, process.SignalAction(pid, signal.SIGINT), ack.Acknowledgement())

    while output is None:
        await handle()

    _, msg = await conn_dealer.recv_msg()
    assert msg == process.ExitUpdate(pid, -signal.SIGINT)
