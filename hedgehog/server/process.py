import zmq
import subprocess, selectors, threading
from hedgehog.protocol.messages.process import STDIN, STDOUT, STDERR

EXIT = 0xFF


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

    def __init__(self, *args):
        """
        Runs a process defined by `args`.

        This will spawn a new process, along with two threads for handling input and output.
        Communication with the process can be done via `socket`, or via the convenience methods `write` and `read`.

        :param args: The command line arguments
        """
        self.context = zmq.Context()

        self.socket = self.context.socket(zmq.PAIR)
        self.socket.bind('inproc://socket')

        signal = self.context.socket(zmq.PAIR)
        signal.bind('inproc://signal')

        self.status = None
        self.proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE
        )

        threading.Thread(target=self._writer).start()
        signal.recv()
        signal.close()

        threading.Thread(target=self._reader).start()

    def _writer(self):
        out = self.context.socket(zmq.PAIR)
        out.bind('inproc://out')

        signal = self.context.socket(zmq.PAIR)
        signal.connect('inproc://signal')
        signal.send(b'')
        signal.close()

        socket = self.context.socket(zmq.PAIR)
        socket.connect('inproc://socket')

        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        poller.register(out, zmq.POLLIN)

        while len(poller.sockets) > 0:
            for sock, _ in poller.poll():
                fileno, msg = sock.recv_multipart()
                if fileno[0] == STDIN:
                    # write message to stdin
                    file = self.proc.stdin
                    if msg != b'':
                        file.write(msg)
                        file.flush()
                    else:
                        file.close()
                        poller.unregister(sock)
                elif fileno[0] in {STDOUT, STDERR}:
                    # pipe message to the socket
                    socket.send_multipart([fileno, msg])
                elif fileno[0] == EXIT:
                    # pipe message to the socket, unregister sock
                    socket.send_multipart([fileno, msg])
                    poller.unregister(sock)
                    sock.close()
        socket.close()

    def _reader(self):
        out = self.context.socket(zmq.PAIR)
        out.connect('inproc://out')

        selector = selectors.DefaultSelector()
        selector.register(self.proc.stdout, selectors.EVENT_READ, STDOUT)
        selector.register(self.proc.stderr, selectors.EVENT_READ, STDERR)

        while len(selector.get_map()) > 0:
            for key, _ in selector.select():
                fileno, file = key.data, key.fileobj

                data = None
                while data != b'':
                    data = file.read(4096)
                    if data is None:
                        break
                    out.send_multipart([bytes([fileno]), data])

                if data == b'':
                    selector.unregister(file)
                    file.close()

        self.status = self.proc.wait()
        assert 0 <= self.status < 256
        out.send_multipart([bytes([EXIT]), self.status.to_bytes(1, 'big')])
        out.close()
        selector.close()

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
        fileno, msg = self.socket.recv_multipart()
        fileno = fileno[0]
        if fileno == EXIT:
            return None
        else:
            return fileno, msg
