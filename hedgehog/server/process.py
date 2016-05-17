import zmq
import fcntl, os, subprocess, threading
from hedgehog.protocol.messages.process import STDIN, STDOUT, STDERR

EXIT = 0xFF
SIGNAL = 0xFE


class Process:
    """
    `Process` provides a ZMQ-based abstraction around a `Popen` object.

    Using the `Process` class, users can (and must) interact with a child process via a single ZMQ socket, `socket`.
    Messages are multipart, consisting of `(fileno, msg)`,
    where `fileno` consists of one byte `STDIN`, `STDOUT`, `STDERR`, `EXIT`.
    `STDIN` may be used to sending data to the process' `stdin` stream,
    `STDOUT` and `STDERR` for receiving data from the corresponding streams,
    and `EXIT` indicates the process has finished.
    `msg` will contain a single byte that is the exit `status` of the process (`0 <= status < 256`).
    (The `status` will also be available as a field.)

    Data is sent as soon as it is available, i.e. not only after a full line or a fixed number of bytes.
    The fragmentation of stream data into ZMQ messages is arbitrary, but a limited per-message length can be assumed.
    An empty `msg` (`b''`) denotes EOF, both for reading and writing.
    The `EXIT` message will always be the last message received from the socket;
    at that point, both output streams will have reached EOF
    and the process will have finished with the indicated `status`.



    Returned is the `Popen` object, three ZMQ sockets for piping `stdin`, `stdout`, and `stderr`,
    and a socket that reports the process' exit status.
    The `Popen` object's file objects must not be used; use the ZMQ sockets!

    Data is sent as soon as it is available, i.e. not only after a full line or a fixed number of bytes.
    The fragmentation of stream data into ZMQ messages is arbitrary, but a limited per-message length can be assumed.

    To denote EOF, empty ZMQ frames are used.
    That means, sending `b''` to `sdtin` will close the underlying file,
    and receiving `b''` from stdout or stderr means that EOF for the underlying file was reached.

    :param args: The command line arguments
    :return: a tuple `(proc, stdin, stdout, stderr, exit)`
    """

    def __init__(self, *args, **kwargs):
        """
        Runs a process defined by `args`.

        This will spawn a new process, along with two threads for handling input and output.
        Communication with the process can be done via `socket`, or via the convenience methods `write` and `read`.

        :param args: The command line arguments
        """
        ctx = zmq.Context()

        self.socket = ctx.socket(zmq.PAIR)
        self.socket.bind('inproc://socket')

        self.status = None
        self.proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            **kwargs
        )

        def poll():
            socket = ctx.socket(zmq.PAIR)
            socket.connect('inproc://socket')

            poller = zmq.Poller()
            handlers = {}

            def register_input():
                file = self.proc.stdin

                def handler():
                    [fileno], msg = socket.recv_multipart()
                    assert fileno == STDIN

                    if msg != b'':
                        file.write(msg)
                        file.flush()
                    else:
                        poller.unregister(socket)
                        file.close()

                poller.register(socket, zmq.POLLIN)
                handlers[socket] = handler

            def register_output(file, fileno):
                fl = fcntl.fcntl(file, fcntl.F_GETFL)
                fcntl.fcntl(file, fcntl.F_SETFL, fl | os.O_NONBLOCK)

                real_fileno = file.fileno()

                def handler():
                    data = file.read(4096)

                    socket.send_multipart([bytes([fileno]), data])
                    if data == b'':
                        poller.unregister(real_fileno)
                        file.close()

                poller.register(real_fileno, zmq.POLLIN)
                handlers[real_fileno] = handler

            register_input()
            register_output(self.proc.stdout, STDOUT)
            register_output(self.proc.stderr, STDERR)

            while len(poller.sockets) > 0:
                for sock, _ in poller.poll():
                    handlers[sock]()

            self.status = self.proc.wait()
            code, status = (EXIT, self.status) if self.status >= 0 else (SIGNAL, -self.status)
            assert status < 256, self.status
            socket.send_multipart([bytes([code]), status.to_bytes(1, 'big')])
            socket.close()

        threading.Thread(target=poll).start()

    def write(self, fileno, msg=b''):
        """
        Writes `msg` to the child process' file `fileno`.

        An empty `msg` (the default) denotes EOF.

        :param fileno: Must be `STDIN`
        :param msg: The data to write
        """
        self.socket.send_multipart([bytes([fileno]), msg])

    def read(self):
        """
        Reads from the child process' streams.

        If the message received from the socket is `EXIT`, `None` is returned;
        otherwise, the return value is a tuple `(fileno, msg)`, where `fileno` is either `STDOUT` or `STDERR`.

        Note that calling `read` after `EXIT` was received will block indefinitely,
        and that this method will not close the underlying socket on `EXIT`.

        :return: `None`, or `(fileno, msg)`
        """
        [fileno], msg = self.socket.recv_multipart()
        if fileno == EXIT or fileno == SIGNAL:
            return None
        else:
            return fileno, msg
