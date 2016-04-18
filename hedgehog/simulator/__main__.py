import zmq
from . import HedgehogSimulator


def main():
    context = zmq.Context.instance()

    simulator = HedgehogSimulator('tcp://*:5555', context=context)
    simulator.start()


if __name__ == '__main__':
    main()
