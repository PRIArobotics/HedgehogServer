import logging
import zmq
from hedgehog.utils.discovery.node import Node

from . import handlers
from .hedgehog_server import HedgehogServer
from .handlers.hardware import HardwareHandler
from .handlers.process import ProcessHandler
from .hardware.serial import SerialHardwareAdapter

logger = logging.getLogger(__name__)


def start(name, hardware, port=0):
    ctx = zmq.Context.instance()
    service = 'hedgehog_server'

    # TODO retry on missing network
    node = Node(name, ctx)
    node.start()
    node.join(service)

    handler = handlers.to_dict(HardwareHandler(hardware()), ProcessHandler())

    server = HedgehogServer('tcp://*:{}'.format(port), handler, ctx=ctx)
    node.add_service(service, server.socket)

    logger.info("{} started on {}".format(node.name(), server.socket.last_endpoint.decode('utf-8')))
