import argparse
import configparser
import logging
import logging.config
import os.path
import socket
import subprocess
import time
import zmq
from hedgehog.utils.discovery.service_node import ServiceNode
from pyre import zhelper

from . import handlers
from .hedgehog_server import HedgehogServer
from .handlers.hardware import HardwareHandler
from .handlers.process import ProcessHandler
from .hardware.serial import SerialHardwareAdapter

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--name', default=None,
                        help="Node name for discovery; can use {mode} and {mac} "
                             "to include server/simulator and MAC address. Default: 'Hedgehog {mode} {mac}'")
    parser.add_argument('-p', '--port', type=int, default=0,
                        help="The port to use, 0 means random port; default: %(default)s")
    parser.add_argument('--svc', '--service', dest='services', action='append', default=['hedgehog_server'],
                        help="Additional service identifiers, may appear multiple times; "
                             "%(default)s is always registered")
    parser.add_argument('--scan', '--scan-config', dest='scan_config', action='store_true',
                        help="If given, a config file will be processed at startup if it exists")
    parser.add_argument('--scan-file', '--scan-config-file', dest='scan_config_file', default='/media/usb/hedgehog.conf',
                        help="Location of the config file to scan; default: %(default)s")
    parser.add_argument('--config-file', dest='config_file', default='hedgehog.conf',
                        help="The hedgehog config file; default: %(default)s")
    parser.add_argument('--logging-conf', dest='logging_conf',
                        help="If given, logging is configured from this file")
    return parser.parse_args()


def name_fmt_kwargs(simulator=False):
    # get dict of interfaces
    netinf = {iface: data
              for netinf in zhelper.get_ifaddrs()
              for iface, data in netinf.items()}

    # consider only wired interfaces - TODO cross platform testing
    wired = {iface: data
             for iface, data in netinf.items()
             if any(iface.startswith(prefix) for prefix in ('eth', 'en'))}

    # get MAC addresses, predictably sorted by interface name
    addrs = [data[socket.AF_PACKET]['addr']
             for iface, data in sorted(netinf.items())
             if socket.AF_PACKET in data]

    # choose first interface, use last three octets in server name
    return {
        'mode': "Simulator" if simulator else "Server",
        'mac': addrs[0][9:] if addrs else "",
    }


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
    args = parse_args()

    if args.logging_conf:
        logging.config.fileConfig(args.logging_conf)

    config = configparser.ConfigParser()
    config.read(args.config_file)

    if args.scan_config and os.path.isfile(args.scan_config_file):
        scan_config = configparser.ConfigParser()
        scan_config.read(args.scan_config_file)

        apply_scan_config(config, scan_config)

        with open(args.config_file, mode='w') as f:
            config.write(f)

    from hedgehog.server.hardware.simulated import SimulatedHardwareAdapter
    name = args.name or config.get('default', 'name', fallback='Hedgehog {mode} {mac}')
    name = name.format(**name_fmt_kwargs(hardware == SimulatedHardwareAdapter))
    port = args.port or config.getint('default', 'port', fallback=0)
    services = set()
    services.update(args.services)
    services.update(config.get('default', 'services', fallback='').split())

    start(hardware, name=name, port=port, services=services)


def start(hardware, name=None, port=0, services={'hedgehog_server'}):
    ctx = zmq.Context.instance()

    handler = handlers.to_dict(HardwareHandler(hardware()), ProcessHandler())

    server = HedgehogServer(ctx, 'tcp://*:{}'.format(port), handler)
    with server:
        logger.info("{} started on {}".format(name, server.socket.last_endpoint.decode('utf-8')))

        # TODO clean way to exit
        while True:
            logger.info("Starting Node for discovery...")

            node = ServiceNode(ctx, name)
            with node:
                for service in services:
                    node.join(service)
                    node.add_service(service, server.socket)

                while True:
                    command, *args = node.evt_pipe.recv_multipart()
                    if command == b'BEACON TERM':
                        logger.info("Beacon terminated (network gone?). Retry in 3 seconds...")
                        time.sleep(3)
                        node.restart_beacon()
                    if command == b'$TERM':
                        break

            logger.info("Node terminated. Retry in 3 seconds...")
            time.sleep(3)
