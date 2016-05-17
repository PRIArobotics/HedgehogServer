import zmq
from hedgehog.server import HedgehogServer
from hedgehog.server import handlers
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter


def handler():
    return handlers.to_dict(HardwareHandler(SimulatedHardwareAdapter()), ProcessHandler())


def main():
    ctx = zmq.Context.instance()

    server = HedgehogServer('tcp://*:5555', handler(), ctx=ctx)


if __name__ == '__main__':
    main()
