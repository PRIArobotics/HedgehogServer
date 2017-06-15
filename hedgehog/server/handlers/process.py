from typing import Dict

from hedgehog.protocol.errors import FailedCommandError
from hedgehog.protocol.messages import ack, process, motor
from hedgehog.server.process import Process
from hedgehog.server.handlers import CommandHandler, command_handlers


class ProcessHandler(CommandHandler):
    _handlers, _command = command_handlers()

    def __init__(self, adapter):
        super().__init__()
        self._processes = {}  # type: Dict[int, Process]
        self.adapter = adapter

    @_command(process.ExecuteAction)
    def process_execute_action(self, server, ident, msg):
        proc = Process(*msg.args, cwd=msg.working_dir)
        pid = proc.proc.pid
        self._processes[pid] = proc

        def cb():
            _msg = proc.read()
            if _msg is None:
                server.send_async(ident, process.ExitUpdate(pid, proc.returncode))

                # turn off all actuators
                for port in range(4):
                    self.adapter.set_motor(port, motor.POWER, 0)
                for port in range(4):
                    self.adapter.set_servo(port, False, 0)

                server.unregister(proc.socket)
                del self._processes[pid]
            else:
                fileno, msg = _msg
                server.send_async(ident, process.StreamUpdate(pid, fileno, msg))

        server.register(proc.socket, cb)
        return process.ExecuteReply(pid)

    @_command(process.StreamAction)
    def process_stream_action(self, server, ident, msg):
        # check whether the process has already finished
        if msg.pid in self._processes:
            proc = self._processes[msg.pid]
            proc.write(msg.fileno, msg.chunk)
            return ack.Acknowledgement()
        else:
            raise FailedCommandError("no process with pid {}".format(msg.pid))

    @_command(process.SignalAction)
    def process_signal_action(self, server, ident, msg):
        # check whether the process has already finished
        if msg.pid in self._processes:
            proc = self._processes[msg.pid]
            proc.send_signal(msg.signal)
            return ack.Acknowledgement()
        else:
            raise FailedCommandError("no process with pid {}".format(msg.pid))
