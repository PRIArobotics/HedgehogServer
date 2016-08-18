import logging.config
import sys

from hedgehog.server import parse_args, start
from hedgehog.server.hardware.serial import SerialHardwareAdapter


def main():
    logging.config.fileConfig('logging.conf')

    args = parse_args()
    start(args.name, SerialHardwareAdapter, port=args.port, services=args.services)


if __name__ == '__main__':
    main()
