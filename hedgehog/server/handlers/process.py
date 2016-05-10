from hedgehog.protocol.messages import ack, process
from hedgehog.server.process import Process
from hedgehog.server.handlers import CommandHandler, command_handlers


class ProcessHandler(CommandHandler):
    _handlers, _command = command_handlers()

    def __init__(self):
        super().__init__()
        self._processes = {}

    @_command(process.ExecuteRequest)
    def process_execute_request(self, server, ident, msg):
        proc = Process(*msg.args, cwd=msg.working_dir)
        pid = proc.proc.pid
        self._processes[pid] = proc

        def cb():
            msg = proc.read()
            if msg is None:
                msg = process.ExitUpdate(pid, proc.status)
                server.socket.send(ident, msg)
                server.unregister(proc.socket)
                proc.socket.close()
                del self._processes[pid]
            else:
                fileno, msg = msg
                msg = process.StreamUpdate(pid, fileno, msg)
                server.socket.send(ident, msg)

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
            return ack.Acknowledgement(ack.FAILED_COMMAND, "no process with pid {}".format(msg.pid))
