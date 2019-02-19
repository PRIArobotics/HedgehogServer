from typing import List, Tuple

import asyncio
import trio
import serial
import serial_asyncio
from hedgehog.platform import Controller
from hedgehog.protocol.errors import FailedCommandError
from . import HardwareAdapter, POWER

# commands:
IO_STATE = 0x10
ANALOG_REQ = 0x20
DIGITAL_REQ = 0x30
MOTOR = 0x40
SERVO = 0x50
SERIAL = 0x60
# replies:
OK = 0x80
UNKNOWN_OPCODE = 0x81
INVALID_OPCODE = 0x82
INVALID_PORT = 0x83
INVALID_IO = 0x84
INVALID_MODE = 0x85
INVALID_FLAGS = 0x86
INVALID_VALUE = 0x87
ANALOG_REP = 0xA1
DIGITAL_REP = 0xB1
SERIAL_UPDATE = 0xE1
# number analog and digital ports together, servo and motor ports separately
# special analog ports:
BATTERY_VOLTAGE = 0x80
# special digital ports (output only):
LED1 = 0x90
LED2 = 0x91
BUZZER = 0x92
# serial ports:
SPI1 = 0x00
SPI2 = 0x01

_replies = {
    OK,
    ANALOG_REP,
    DIGITAL_REP,
}
_errors = {
    UNKNOWN_OPCODE,
    INVALID_OPCODE,
    INVALID_PORT,
    INVALID_IO,
    INVALID_MODE,
    INVALID_FLAGS,
    INVALID_VALUE,
}
_cmd_lengths = {
    OK: 1,
    UNKNOWN_OPCODE: 1,
    INVALID_OPCODE: 1,
    INVALID_PORT: 1,
    INVALID_IO: 1,
    INVALID_MODE: 1,
    INVALID_FLAGS: 1,
    INVALID_VALUE: 1,
    ANALOG_REP: 4,
    DIGITAL_REP: 3,
}


class TruncatedcommandError(FailedCommandError):
    pass


async def open_serial_connection(ser: serial.Serial, *,
                                 loop: asyncio.AbstractEventLoop=None, limit: int=asyncio.streams._DEFAULT_LIMIT
                                 ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    if loop is None:
        loop = asyncio.get_event_loop()

    reader = asyncio.StreamReader(limit=limit, loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    transport = serial_asyncio.SerialTransport(loop, protocol, ser)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


class SerialHardwareAdapter(HardwareAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.controller = Controller()
        self.reader = None  # type: asyncio.StreamReader
        self.writer = None  # type: asyncio.StreamWriter
        self.controller.reset(True)

    async def __aenter__(self):
        await super().__aenter__()
        self.reader, self.writer = await open_serial_connection(self.controller.serial)
        # TODO register cleanup

    async def repeatable_command(self, cmd: List[int], reply_code: int=OK, tries: int=3) -> List[int]:
        for i in range(tries - 1):
            try:
                return await self.command(cmd, reply_code=reply_code)
            except TruncatedcommandError:
                pass
        return await self.command(cmd, reply_code=reply_code)

    async def command(self, cmd: List[int], reply_code: int=OK) -> List[int]:
        async def read_command() -> List[int]:
            cmd = await self.reader.read(1)
            if cmd[0] in _cmd_lengths:
                length = _cmd_lengths[cmd[0]]
                if length > 1:
                    cmd += await self.reader.read(length - 1)
            else:
                cmd += await self.reader.read(1)
                length = cmd[1] + 2
                if length > 2:
                    cmd += await self.reader.read(length - 2)
            if len(cmd) != length:
                raise TruncatedcommandError("HWC sent a truncated response")
            return list(cmd)

        self.writer.write(bytes(cmd))
        reply = await read_command()
        while reply[0] not in _replies | _errors:
            # TODO do something with the update
            reply = await read_command()

        if reply[0] in _replies:
            assert reply[0] == reply_code
            return reply
        else:
            if reply[0] == UNKNOWN_OPCODE:
                self.controller.serial.flushOutput()
                await trio.sleep(0.02)
                self.controller.serial.flushInput()
                raise FailedCommandError("opcode unknown to the HWC; connection was reset")
            elif reply[0] == INVALID_OPCODE:
                raise FailedCommandError("opcode not supported")
            elif reply[0] == INVALID_PORT:
                raise FailedCommandError("port not supported or out of range")
            elif reply[0] == INVALID_IO:
                raise FailedCommandError("sensor request invalid for output port")
            elif reply[0] == INVALID_MODE:
                raise FailedCommandError("unsupported motor mode")
            elif reply[0] == INVALID_FLAGS:
                raise FailedCommandError("unsupported combination of IO flags")
            elif reply[0] == INVALID_VALUE and cmd[0] == MOTOR:
                raise FailedCommandError("unsupported motor power/velocity")
            elif reply[0] == INVALID_VALUE and cmd[0] == SERVO:
                raise FailedCommandError("unsupported servo position")
            raise FailedCommandError("unknown hardware controller response")

    async def set_io_state(self, port, flags):
        await self.repeatable_command([IO_STATE, port, flags])

    async def get_analog(self, port):
        _, port_, value_hi, value_lo = await self.repeatable_command([ANALOG_REQ, port], ANALOG_REP)
        assert port_ == port
        return int.from_bytes([value_hi, value_lo], 'big')

    async def get_digital(self, port):
        _, port_, value = await self.repeatable_command([DIGITAL_REQ, port], DIGITAL_REP)
        assert port_ == port
        assert value & ~0x01 == 0x00
        return value != 0

    async def set_motor(self, port, state, amount=0, reached_state=POWER, relative=None, absolute=None):
        if not -0x8000 < amount < 0x8000:
            raise FailedCommandError("unsupported motor power/velocity")
        value = amount if amount > 0 else (0x8000 | -amount)
        value_hi, value_lo = value.to_bytes(2, 'big')
        await self.repeatable_command([MOTOR, port, state, value_hi, value_lo])

    async def set_servo(self, port, active, position):
        if not 0 <= position < 0x8000:
            raise FailedCommandError("unsupported servo position")
        value = position | (0x8000 if active else 0x0000)
        value_hi, value_lo = value.to_bytes(2, 'big')
        await self.repeatable_command([SERVO, port, value_hi, value_lo])
