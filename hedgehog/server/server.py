from hedgehog.server import launch
from hedgehog.server.hardware.serial import SerialHardwareAdapter


def main():
    launch(SerialHardwareAdapter)


if __name__ == '__main__':
    main()
