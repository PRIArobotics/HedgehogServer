import argparse
import configparser
import logging
import logging.config
import os.path
import subprocess
import trio
import trio_asyncio
from contextlib import suppress

from hedgehog.utils.zmq import trio as zmq_trio

from . import handlers
from .hedgehog_server import HedgehogServer
from .handlers.hardware import HardwareHandler
from .handlers.process import ProcessHandler

logger = logging.getLogger(__name__)


def parse_args(simulator=False):
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, default=0,
                        help="The port to use, 0 means random port; default: %(default)s")
    parser.add_argument('--scan', '--scan-config', dest='scan_config', action='store_true',
                        help="If given, a config file will be processed at startup if it exists")
    parser.add_argument('--scan-file', '--scan-config-file', dest='scan_config_file', default='/media/usb/hedgehog.conf',
                        help="Location of the config file to scan; default: %(default)s")
    parser.add_argument('--config-file', dest='config_file', default='hedgehog.conf',
                        help="The hedgehog config file; default: %(default)s")
    parser.add_argument('--logging-conf', dest='logging_conf',
                        help="If given, logging is configured from this file")
    if simulator:
        parser.add_argument('--sensors', '--simulate-sensors', dest='simulate_sensors', action='store_true',
                            help="If given, noisy sensor readings will be simulated")
    return parser.parse_args()


def apply_scan_config(config, scan_config):
    def set(config, section, option, value):
        if value is not None:
            if section not in config:
                config.add_section(section)
            config[section][option] = value
        elif section in config and option in config[section]:
            del config[section][option]
            if not config[section]:
                del config[section]

    def get(config, section, option):
        return config.get(section, option, fallback=None)

    def copy(in_cfg, out_cfg, section, option):
        value = get(in_cfg, section, option)
        set(out_cfg, section, option, value)
        return value

    copy(scan_config, config, 'default', 'name')
    copy(scan_config, config, 'default', 'port')
    copy(scan_config, config, 'default', 'services')
    wifi_commands = get(scan_config, 'wifi', 'commands')

    if wifi_commands:
        # TODO make this less dangerous when called on a non-hedgehog machine
        subprocess.Popen(['wpa_cli'], stdin=subprocess.PIPE).communicate(wifi_commands.encode())


def launch(hardware):
    from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter
    simulator = hardware == SimulatedHardwareAdapter

    args = parse_args(simulator)

    if args.logging_conf:
        logging.config.fileConfig(args.logging_conf)

    if simulator and args.simulate_sensors:
        _hardware = hardware

        def hardware(*args, **kwargs):
            return _hardware(*args, simulate_sensors=True, **kwargs)

    config = configparser.ConfigParser()
    config.read(args.config_file)

    if args.scan_config and os.path.isfile(args.scan_config_file):
        scan_config = configparser.ConfigParser()
        scan_config.read(args.scan_config_file)

        apply_scan_config(config, scan_config)

        with open(args.config_file, mode='w') as f:
            config.write(f)

    port = args.port or config.getint('default', 'port', fallback=0)

    with suppress(KeyboardInterrupt):
        start(hardware, port)


def start(hardware, port=0):
    ctx = zmq_trio.Context.instance()

    async def run():
        adapter = hardware()
        handler = handlers.merge(HardwareHandler(adapter), ProcessHandler(adapter))

        async with trio_asyncio.open_loop(), adapter:
            await HedgehogServer(ctx, 'tcp://*:{}'.format(port), handler).run()

    trio.run(run)
