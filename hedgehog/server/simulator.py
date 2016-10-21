from hedgehog.server import launch
from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter


def main():
    launch(SimulatedHardwareAdapter)


if __name__ == '__main__':
    main()
