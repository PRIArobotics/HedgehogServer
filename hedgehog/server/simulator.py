import sys
import zmq
from hedgehog.utils.discovery.node import Node
from hedgehog.server import HedgehogServer
from hedgehog.server import handlers
from hedgehog.server.handlers.hardware import HardwareHandler
from hedgehog.server.handlers.process import ProcessHandler
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter
from hedgehog.server.hardware.logging import LoggingHardwareAdapter


def handler():
    hardware = SimulatedHardwareAdapter()
    hardware = LoggingHardwareAdapter(hardware)
    return handlers.to_dict(HardwareHandler(hardware), ProcessHandler())


def main():
    args = sys.argv[1:]
    port = 0 if len(args) == 0 else args[0]

    ctx = zmq.Context.instance()
    service = 'hedgehog_server'

    node = Node("Hedgehog Simulator", ctx)
    node.start()
    node.join(service)

    server = HedgehogServer('tcp://*:{}'.format(port), handler(), ctx=ctx)
    node.add_service(service, server.socket.socket)

    print("{} started on {}".format(node.name(), server.socket.socket.last_endpoint.decode('utf-8')))


if __name__ == '__main__':
    main()
