import zmq
import fcntl
import os
import subprocess
import threading

from hedgehog.protocol.messages.process import STDIN, STDOUT, STDERR
from hedgehog.utils.zmq.pipe import pipe
from hedgehog.utils.zmq.poller import Poller


class Process:
    """
    `Process` provides a ZMQ-based abstraction around a `Popen` object.

    Using the `Process` class, users can (and must) interact with a child process via a single ZMQ socket, `socket`.
    Messages are multipart, consisting of command and payload, where valid commands are `b'READ'`. `b'WRITE'` and
    `b'EXIT'`. For read and write, the payload consists of `fileno` (`STDIN` for writing, one of `STDOUT` or `STDERR`
    for reading) and a `chunk`; an exit message has no payload, but the `returncode` attribute will be set.
    In addition to these messages, signals may be sent to the process directly, e.g. to kill it.

    Data is sent as soon as it is available, i.e. not only after a full line or a fixed number of bytes.
    The fragmentation of stream data into ZMQ messages is arbitrary, but a limited per-message length can be assumed.
    An empty `chunk` (`b''`) denotes EOF, both for reading and writing.

    The `EXIT` message will always be the last message received from the socket; at that point, both output streams will
    have reached EOF and the process will have finished with the indicated `status`.
    """

    def __init__(self, *args, **kwargs):
        """
        Runs a process defined by `args`.

        This will spawn a new process, along with two threads for handling input and output.
        Communication with the process can be done via `socket`, or via the convenience methods `write` and `read`.

        :param args: The command line arguments
        """
        ctx = zmq.Context()

        self.socket, socket = pipe(ctx)

        self.returncode = None
        self.proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            **kwargs
        )

        poller = Poller()

        def register_input():
            file = self.proc.stdin

            def handler():
                cmd, *msg = socket.recv_multipart()
                assert cmd == b'WRITE'
                [fileno], chunk = msg
                assert fileno == STDIN

                if chunk != b'':
                    file.write(chunk)
                    file.flush()
                else:
                    poller.unregister(socket)
                    file.close()

            poller.register(socket, zmq.POLLIN, handler)

        def register_output(file, fileno):
            fl = fcntl.fcntl(file, fcntl.F_GETFL)
            fcntl.fcntl(file, fcntl.F_SETFL, fl | os.O_NONBLOCK)

            real_fileno = file.fileno()

            def handler():
                chunk = file.read(4096)

                socket.send_multipart([b'READ', bytes([fileno]), chunk])
                if chunk == b'':
                    poller.unregister(real_fileno)
                    file.close()

            poller.register(real_fileno, zmq.POLLIN, handler)

        register_input()
        register_output(self.proc.stdout, STDOUT)
        register_output(self.proc.stderr, STDERR)

        def poll():
            while len(poller.sockets) > 0:
                for _, _, handler in poller.poll():
                    handler()

            self.returncode = self.proc.wait()
            socket.send(b'EXIT')
            socket.close()

        threading.Thread(target=poll).start()

    def send_signal(self, signal):
        """
        Sends `signal` to the child process.

        :param signal: The signal to be sent
        """
        self.proc.send_signal(signal)

    def write(self, fileno, chunk=b''):
        """
        Writes `chunk` to the child process' file `fileno`.

        An empty `chunk` (the default) denotes EOF.

        :param fileno: Must be `STDIN`
        :param chunk: The data to write
        """
        self.socket.send_multipart([b'WRITE', bytes([fileno]), chunk])

    def read(self):
        """
        Reads from the child process' streams.

        If the message received from the socket is `b'EXIT'`, `None` is returned;
        otherwise, the return value is a tuple `(fileno, chunk)`, where `fileno` is either `STDOUT` or `STDERR`.

        Note that calling `read` after `b'EXIT'` was received will block indefinitely,
        and that this method will not close the underlying socket on `b'EXIT'`.

        :return: `None`, or `(fileno, chunk)`
        """
        cmd, *msg = self.socket.recv_multipart()
        if cmd == b'READ':
            [fileno], chunk = msg
            return fileno, chunk
        elif cmd == b'EXIT':
            return None
        else:
            assert False
