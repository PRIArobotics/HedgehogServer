from hedgehog.protocol.messages import process
from hedgehog.server.process import run
from hedgehog.server.handlers import CommandHandler, command_handlers


class ProcessHandler(CommandHandler):
    _handlers, _command = command_handlers()

    def __init__(self):
        super().__init__()
        self._processes = {}

    @_command(process.ExecuteRequest)
    def process_execute_request(self, server, ident, msg):
        proc, stdin, stdout, stderr, exit = run(*msg.args)
        pid = proc.pid
        self._processes[pid] = proc, (stdin, stdout, stderr), exit

        def read_cb(socket, fileno):
            def cb():
                chunk = socket.recv()
                msg = process.StreamUpdate(pid, fileno, chunk)
                server.socket.send(ident, msg)
            return cb

        def exit_cb(socket):
            def cb():
                status = int.from_bytes(socket.recv(), 'big')
                msg = process.ExitUpdate(pid, status)
                server.socket.send(ident, msg)
                del self._processes[pid]
            return cb

        server.register(stdout, read_cb(stdout, process.STDOUT))
        server.register(stderr, read_cb(stderr, process.STDERR))
        server.register(exit, exit_cb(exit))
        server.socket.send(ident, process.ExecuteReply(pid))

    @_command(process.StreamAction)
    def process_stream_action(self, server, ident, msg):
        _, sockets, _ = self._processes[msg.pid]
        sockets[msg.fileno].send(msg.chunk)
