from hedgehog.protocol.messages import analog, digital, motor, servo, process
from hedgehog.server.process import run
from hedgehog.server.handlers import CommandHandler, command_handlers, to_dict


class SimulatorHandler(CommandHandler):
    _handlers, _command = command_handlers()

    def __init__(self):
        super().__init__()
        self._processes = {}

    @_command(analog.Request)
    def analog_request(self, server, ident, msg):
        server.socket.send(ident, analog.Update(msg.port, 0))

    @_command(analog.StateAction)
    def analog_state_action(self, server, ident, msg):
        # TODO set analog pullup
        pass

    @_command(digital.Request)
    def digital_request(self, server, ident, msg):
        server.socket.send(ident, digital.Update(msg.port, False))

    @_command(digital.StateAction)
    def digital_state_action(self, server, ident, msg):
        # TODO set digital pullup, output
        pass

    @_command(digital.Action)
    def digital_action(self, server, ident, msg):
        # TODO set digital pullup, output
        pass

    @_command(motor.Action)
    def motor_action(self, server, ident, msg):
        # TODO set motor action
        pass

    @_command(motor.Request)
    def motor_request(self, server, ident, msg):
        server.socket.send(ident, motor.Update(msg.port, 0, 0))

    @_command(motor.SetPositionAction)
    def motor_set_position_action(self, server, ident, msg):
        # TODO set motor position
        pass

    @_command(servo.Action)
    def servo_action(self, server, ident, msg):
        # TODO set servo position
        pass

    @_command(servo.StateAction)
    def servo_state_action(self, server, ident, msg):
        # TODO set servo active
        pass

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


def handlers():
    return to_dict(SimulatorHandler())


def main():
    import zmq
    from hedgehog.server import HedgehogServer

    context = zmq.Context.instance()

    simulator = HedgehogServer('tcp://*:5555', SimulatorCommandHandler(), context=context)
    simulator.start()


if __name__ == '__main__':
    main()
