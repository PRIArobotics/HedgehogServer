import zmq
import subprocess, fcntl, selectors
import os, threading


def run(*args):
    """
    Runs a process defined by `args`.

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

    def write_handler(context, pipes):
        poller = zmq.Poller()

        files = {}

        for _, file, endpoint in pipes:
            socket = context.socket(zmq.PAIR)
            socket.connect(endpoint)
            poller.register(socket, zmq.POLLIN)
            files[socket] = file

        while len(poller.sockets) > 0:
            for socket, _ in poller.poll():
                file = files[socket]

                msg = socket.recv()
                if msg != b'':
                    file.write(msg)
                    file.flush()
                else:
                    poller.unregister(socket)
                    file.close()
                    socket.close()

    def read_handler(context, proc, exit_endpoint, pipes):
        selector = selectors.DefaultSelector()

        for _, file, endpoint in pipes:
            flags = fcntl.fcntl(file, fcntl.F_GETFL)
            flags |= os.O_NONBLOCK
            fcntl.fcntl(file, fcntl.F_SETFL, flags)

            socket = context.socket(zmq.PAIR)
            socket.connect(endpoint)
            selector.register(file, selectors.EVENT_READ, socket)

        exit = context.socket(zmq.PAIR)
        exit.connect(exit_endpoint)

        while len(selector.get_map()) > 0:
            for key, _ in selector.select():
                socket, file = key.data, key.fileobj

                data = None
                while data != b'':
                    data = file.read(4096)
                    if data is None:
                        break
                    socket.send(data)

                if data == b'':
                    selector.unregister(file)
                    file.close()
                    socket.close()

        status = proc.wait()
        assert 0 <= status < 256
        exit.send(status.to_bytes(1, 'big'))
        exit.close()

    def pipe(context, file, endpoint):
        socket = context.socket(zmq.PAIR)
        socket.bind(endpoint)
        return socket, file, endpoint

    context = zmq.Context()
    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE
    )

    stdin = pipe(context, proc.stdin, 'inproc://stdin')
    stdout = pipe(context, proc.stdout, 'inproc://stdout')
    stderr = pipe(context, proc.stderr, 'inproc://stderr')
    exit, _, exit_endpoint = pipe(context, None, 'inproc://exit')

    threading.Thread(target=write_handler, args=[context, [stdin]]).start()
    threading.Thread(target=read_handler, args=[context, proc, exit_endpoint, [stdout, stderr]]).start()

    stdin, stdout, stderr = (socket for socket, _, _ in (stdin, stdout, stderr))
    return proc, stdin, stdout, stderr, exit
