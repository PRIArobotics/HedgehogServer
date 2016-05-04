from hedgehog.protocol.messages import analog, digital, motor, servo, process
from hedgehog.server.process import run


class SimulatorCommandHandler:
    def __init__(self):
        self._processes = {}

    def analog_request(self, server, ident, msg):
        server.socket.send(ident, analog.Update(msg.port, 0))

    def analog_state_action(self, server, ident, msg):
        # TODO set analog pullup
        pass

    def digital_request(self, server, ident, msg):
        server.socket.send(ident, digital.Update(msg.port, False))

    def digital_state_action(self, server, ident, msg):
        # TODO set digital pullup, output
        pass

    def digital_action(self, server, ident, msg):
        # TODO set digital pullup, output
        pass

    def motor_action(self, server, ident, msg):
        # TODO set motor action
        pass

    def motor_request(self, server, ident, msg):
        server.socket.send(ident, motor.Update(msg.port, 0, 0))

    def motor_set_position_action(self, server, ident, msg):
        # TODO set motor position
        pass

    def servo_action(self, server, ident, msg):
        # TODO set servo position
        pass

    def servo_state_action(self, server, ident, msg):
        # TODO set servo active
        pass

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

    def process_stream_action(self, server, ident, msg):
        _, sockets, _ = self._processes[msg.pid]
        sockets[msg.fileno].send(msg.chunk)


def handlers():
    import inspect
    return dict(inspect.getmembers(SimulatorCommandHandler(), predicate=inspect.ismethod))


def main():
    import zmq
    from hedgehog.server import HedgehogServer

    context = zmq.Context.instance()

    simulator = HedgehogServer('tcp://*:5555', SimulatorCommandHandler(), context=context)
    simulator.start()


if __name__ == '__main__':
    main()
