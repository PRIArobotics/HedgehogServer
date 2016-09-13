import argparse
import logging
import time
import zmq
from hedgehog.utils.discovery.service_node import ServiceNode

from . import handlers
from .hedgehog_server import HedgehogServer
from .handlers.hardware import HardwareHandler
from .handlers.process import ProcessHandler
from .hardware.serial import SerialHardwareAdapter

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name', default='Hedgehog Server',
                        help="Node name for discovery; default: '%(default)s'")
    parser.add_argument('-p', '--port', type=int, default=0,
                        help="The port to use, 0 means random port; default: %(default)s")
    parser.add_argument('--svc', '--service', dest='services', action='append', default=['hedgehog_server'],
                        help="Additional service identifiers, may appear multiple times; %(default)s is always registered")
    parser.add_argument('--logging-conf', dest='logging_conf',
                        help="If given, logging is configured from this file")
    return parser.parse_args()


def start(name, hardware, port=0, services=('hedgehog_server',)):
    ctx = zmq.Context.instance()

    handler = handlers.to_dict(HardwareHandler(hardware()), ProcessHandler())

    server = HedgehogServer(ctx, 'tcp://*:{}'.format(port), handler)
    with server:
        logger.info("{} started on {}".format(name, server.socket.last_endpoint.decode('utf-8')))

        # TODO clean way to exit
        while True:
            logger.info("Starting Node for discovery...")

            node = ServiceNode(ctx, name)
            with node:
                for service in services:
                    node.join(service)
                    node.add_service(service, server.socket)

                while True:
                    command, *args = node.evt_pipe.recv_multipart()
                    if command == b'BEACON TERM':
                        logger.info("Beacon terminated (network gone?). Retry in 3 seconds...")
                        time.sleep(3)
                        node.restart_beacon()
                    if command == b'$TERM':
                        break

            logger.info("Node terminated. Retry in 3 seconds...")
            time.sleep(3)
