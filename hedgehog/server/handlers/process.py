from typing import AsyncIterator, Dict

import asyncio.subprocess
import trio
import trio_asyncio

from hedgehog.protocol.errors import FailedCommandError
from hedgehog.protocol.messages import ack, process, motor

from . import CommandHandler, CommandRegistry
from ..hedgehog_server import Job
from ..hardware import HardwareAdapter


class ProcessHandler(CommandHandler):
    _commands = CommandRegistry()

    def __init__(self, adapter: HardwareAdapter) -> None:
        super().__init__()
        self._processes = {}  # type: Dict[int, asyncio.subprocess.Process]
        self.adapter = adapter

    async def _handle_process(self, server, ident, msg, *, task_status=trio.TASK_STATUS_IGNORED) -> AsyncIterator[Job]:
        proc = await trio_asyncio.aio_as_trio(asyncio.create_subprocess_exec)(
            *msg.args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=msg.working_dir)

        pid = proc.pid
        self._processes[pid] = proc

        task_status.started(pid)

        async with trio.open_nursery() as nursery:
            async def handle_stream(fileno, file):
                while True:
                    chunk = await trio_asyncio.aio_as_trio(file.read)(4096)
                    async with server.job():
                        await server.send_async(ident, process.StreamUpdate(pid, fileno, chunk))
                    if chunk == b'':
                        break

            nursery.start_soon(handle_stream, process.STDOUT, proc.stdout)
            nursery.start_soon(handle_stream, process.STDERR, proc.stderr)

        exit_code = await trio_asyncio.aio_as_trio(proc.wait)()

        try:
            # turn off all actuators
            # TODO hard coded number of ports
            for port in range(4):
                async with server.job():
                    await self.adapter.set_motor(port, motor.POWER, 0)
            for port in range(4):
                async with server.job():
                    await self.adapter.set_servo(port, False, 0)
        except Exception:  # pragma: nocover
            async with server.job():
                await server.send_async(ident, process.ExitUpdate(pid, exit_code))
            raise
        else:
            async with server.job():
                await server.send_async(ident, process.ExitUpdate(pid, exit_code))
        finally:
            del self._processes[pid]

    @_commands.register(process.ExecuteAction)
    async def process_execute_action(self, server, ident, msg):
        pid = await server.add_task(self._handle_process, server, ident, msg)
        return process.ExecuteReply(pid)

    @_commands.register(process.StreamAction)
    async def process_stream_action(self, server, ident, msg):
        # check whether the process has already finished
        if msg.pid in self._processes:
            assert msg.fileno == process.STDIN

            proc = self._processes[msg.pid]
            if msg.chunk != b'':
                proc.stdin.write(msg.chunk)
            else:
                proc.stdin.write_eof()
            return ack.Acknowledgement()
        else:
            raise FailedCommandError("no process with pid {}".format(msg.pid))

    @_commands.register(process.SignalAction)
    async def process_signal_action(self, server, ident, msg):
        # check whether the process has already finished
        if msg.pid in self._processes:
            proc = self._processes[msg.pid]
            proc.send_signal(msg.signal)
            return ack.Acknowledgement()
        else:
            raise FailedCommandError("no process with pid {}".format(msg.pid))
