import logging.config

from hedgehog.server import parse_args, start
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter


def main():
    logging.config.fileConfig('logging.conf')

    args = parse_args()
    start(args.name, SimulatedHardwareAdapter, port=args.port, services=args.services)


if __name__ == '__main__':
    main()
