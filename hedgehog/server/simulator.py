import logging.config

from hedgehog.server import parse_args, start
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter


def main():
    args = parse_args()

    if args.logging_conf:
        logging.config.fileConfig(args.logging_conf)
    start(args.name, SimulatedHardwareAdapter, port=args.port, services=args.services)


if __name__ == '__main__':
    main()
