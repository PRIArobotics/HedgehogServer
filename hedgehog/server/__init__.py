import logging
import time
import zmq
from hedgehog.utils.discovery.node import Node

from . import handlers
from .hedgehog_server import HedgehogServer
from .handlers.hardware import HardwareHandler
from .handlers.process import ProcessHandler
from .hardware.serial import SerialHardwareAdapter

logger = logging.getLogger(__name__)


def start(name, hardware, port=0, services=('hedgehog_server',)):
    ctx = zmq.Context.instance()

    handler = handlers.to_dict(HardwareHandler(hardware()), ProcessHandler())

    server = HedgehogServer('tcp://*:{}'.format(port), handler, ctx=ctx)
    logger.info("{} started on {}".format(name, server.socket.last_endpoint.decode('utf-8')))

    # TODO clean way to exit
    while True:
        logger.info("Starting Node for discovery...")
        node = Node(name, ctx)
        node.start()
        for service in services:
            node.join(service)
            node.add_service(service, server.socket)

        while True:
            event = node.socket().recv_multipart()
            command = event.pop(0).decode('UTF-8')
            if command == "$TERM":
                node.stop()
                break
        logger.info("Node terminated (network gone?). Retry in 3 seconds...")
        time.sleep(3)