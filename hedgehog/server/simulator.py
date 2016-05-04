import zmq
from hedgehog.server import HedgehogServer
from hedgehog.server import handlers
from hedgehog.server.handlers.simulator_handler import SimulatorHandler
from hedgehog.server.handlers.process_handler import ProcessHandler


def handler():
    return handlers.to_dict(SimulatorHandler(), ProcessHandler())


def main():
    context = zmq.Context.instance()

    simulator = HedgehogServer('tcp://*:5555', handler(), context=context)
    simulator.start()


if __name__ == '__main__':
    main()
