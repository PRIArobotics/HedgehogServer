from typing import AsyncIterator, Dict

import asyncio.subprocess
import aiostream

from functools import partial
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

    async def _handle_process(self, server, ident, proc) -> AsyncIterator[Job]:
        pid = proc.pid

        async def handle_stream(fileno, file) -> AsyncIterator[Job]:
            while True:
                chunk = await file.read(4096)
                yield fileno, chunk
                if chunk == b'':
                    break

        async with aiostream.stream.merge(
                handle_stream(process.STDOUT, proc.stdout),
                handle_stream(process.STDERR, proc.stderr)).stream() as streamer:
            async for fileno, chunk in streamer:
                yield partial(server.send_async, ident, process.StreamUpdate(pid, fileno, chunk))

        yield partial(server.send_async, ident, process.ExitUpdate(pid, await proc.wait()))
        del self._processes[pid]

        # turn off all actuators
        # TODO hard coded number of ports
        for port in range(4):
            yield partial(self.adapter.set_motor, port, motor.POWER, 0)
        for port in range(4):
            yield partial(self.adapter.set_servo, port, False, 0)

    @_commands.register(process.ExecuteAction)
    async def process_execute_action(self, server, ident, msg):
        proc = await asyncio.create_subprocess_exec(
            *msg.args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=msg.working_dir)

        pid = proc.pid
        self._processes[pid] = proc

        server.add_job_stream(self._handle_process(server, ident, proc))
        return process.ExecuteReply(pid)

    @_commands.register(process.StreamAction)
    async def process_stream_action(self, server, ident, msg):
        # check whether the process has already finished
        if msg.pid in self._processes:
            if msg.fileno != process.STDIN:
                raise FailedCommandError("Can only write to STDIN stream")

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
