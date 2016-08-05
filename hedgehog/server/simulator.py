import logging.config
import sys

from hedgehog.server import start
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter


def main():
    logging.config.fileConfig('logging.conf')

    args = sys.argv[1:]
    port = 0 if len(args) == 0 else args[0]

    start("Hedgehog Server", SimulatedHardwareAdapter, port)


if __name__ == '__main__':
    main()
