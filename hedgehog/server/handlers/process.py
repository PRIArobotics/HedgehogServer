from typing import AsyncIterator, Awaitable, Dict

import asyncio.subprocess

from functools import partial
from hedgehog.protocol.errors import FailedCommandError
from hedgehog.protocol.messages import ack, process, motor

from . import CommandHandler, command_handlers
from ..hedgehog_server import EventStream
from ..hardware import HardwareAdapter


class ProcessHandler(CommandHandler):
    _handlers, _command = command_handlers()

    def __init__(self, adapter: HardwareAdapter) -> None:
        super().__init__()
        self._processes = {}  # type: Dict[int, asyncio.subprocess.Process]
        self.adapter = adapter

    @_command(process.ExecuteAction)
    async def process_execute_action(self, server, ident, msg):
        proc = await asyncio.create_subprocess_exec(
            *msg.args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=msg.working_dir)

        pid = proc.pid
        self._processes[pid] = proc

        async def proc_events() -> EventStream:

            streams = [(process.STDOUT, proc.stdout), (process.STDERR, proc.stderr)]
            tasks = {fileno: asyncio.ensure_future(file.read(4096)) for fileno, file in streams}

            while tasks:
                done, pending = await asyncio.wait(
                    tasks.values(),
                    return_when=asyncio.FIRST_COMPLETED)

                for fileno, file in streams:
                    if fileno in tasks:
                        task = tasks[fileno]
                        if task in done:
                            chunk = task.result()
                            yield partial(server.send_async, ident, process.StreamUpdate(pid, fileno, chunk))
                            if chunk != b'':
                                tasks[fileno] = asyncio.ensure_future(file.read(4096))
                            else:
                                del tasks[fileno]

            yield partial(server.send_async, ident, process.ExitUpdate(pid, await proc.wait()))
            del self._processes[pid]

            # turn off all actuators
            # TODO hard coded number of ports
            for port in range(4):
                yield partial(self.adapter.set_motor, port, motor.POWER, 0)
            for port in range(4):
                yield partial(self.adapter.set_servo, port, False, 0)

        await server.register(proc_events())
        return process.ExecuteReply(pid)

    @_command(process.StreamAction)
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

    @_command(process.SignalAction)
    async def process_signal_action(self, server, ident, msg):
        # check whether the process has already finished
        if msg.pid in self._processes:
            proc = self._processes[msg.pid]
            proc.send_signal(msg.signal)
            return ack.Acknowledgement()
        else:
            raise FailedCommandError("no process with pid {}".format(msg.pid))
