import zmq
from .controller import HedgehogController


def main():
    context = zmq.Context.instance()

    controller = HedgehogController('tcp://*:5555', context=context)
    controller.start()


if __name__ == '__main__':
    main()
