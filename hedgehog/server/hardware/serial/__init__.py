from typing import List, Tuple

import asyncio
from contextlib import asynccontextmanager

import logging
import trio
import trio_asyncio
import serial
import serial_asyncio
from hedgehog.platform import Controller
from hedgehog.protocol.errors import FailedCommandError
from hedgehog.utils import Registry
from .constants import Command, Reply
from .. import HardwareAdapter, HardwareUpdate, POWER


logger = logging.getLogger(__name__)


class TruncatedCommandError(FailedCommandError):
    pass


@asynccontextmanager
async def open_serial_connection(ser: serial.Serial, *,
                                 loop: asyncio.AbstractEventLoop=None, limit: int=asyncio.streams._DEFAULT_LIMIT
                                 ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    if loop is None:
        loop = asyncio.get_event_loop()

    reader = asyncio.StreamReader(limit=limit, loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    transport = serial_asyncio.SerialTransport(loop, protocol, ser)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    try:
        yield reader, writer
    finally:
        # writer is the one who is handling proper shutdown
        writer.close()
        await trio_asyncio.aio_as_trio(writer.wait_closed)()


class SerialHardwareAdapter(HardwareAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.controller = Controller()
        self.reader = None  # type: asyncio.StreamReader
        self.writer = None  # type: asyncio.StreamWriter
        self._replies_in, self._replies_out = trio.open_memory_channel(10)
        self.controller.reset(True)

    async def __aenter__(self):
        await super().__aenter__()

        self.reader, self.writer = await self._stack.enter_async_context(open_serial_connection(self.controller.serial))

        await self._stack.enter_async_context(self._replies_in)
        await self._stack.enter_async_context(self._replies_out)

        nursery = await self._stack.enter_async_context(trio.open_nursery())
        await nursery.start(self._receiver)

    async def _receiver(self, *, task_status=trio.TASK_STATUS_IGNORED):
        read = trio_asyncio.aio_as_trio(self.reader.read)

        decoders = Registry()

        @decoders.register(Reply.SHUTDOWN)
        def decode_shutdown(cmd: List[int]) -> HardwareUpdate:
            raise NotImplemented

        @decoders.register(Reply.EMERGENCY_STOP)
        def decode_emergency_stop(cmd: List[int]) -> HardwareUpdate:
            raise NotImplemented

        @decoders.register(Reply.MOTOR_DONE_UPDATE)
        def decode_motor_done_update(cmd: List[int]) -> HardwareUpdate:
            raise NotImplemented

        @decoders.register(Reply.UART_UPDATE)
        def decode_uart_update(cmd: List[int]) -> HardwareUpdate:
            raise NotImplemented

        async def read_command() -> List[int]:
            cmd = await read(1)
            if cmd[0] not in (Reply.SUCCESS_REPLIES | Reply.ERROR_REPLIES | Reply.UPDATES):
                raise RuntimeError(f"HWC speaks a language we don't understand: 0x{cmd[0]:02X}")
            if cmd[0] in Reply.LENGTHS:
                length = Reply.LENGTHS[cmd[0]]
                if length > 1:
                    cmd += await read(length - 1)
            else:
                cmd += await read(1)
                length = cmd[1] + 2
                if length > 2:
                    cmd += await read(length - 2)
            if len(cmd) != length:
                raise TruncatedCommandError(f"HWC sent a truncated response: {' '.join(f'{b:02X}' for b in cmd)}")
            return list(cmd)

        task_status.started()

        while True:
            # TODO handle TruncatedCommandError
            # TODO will this abort when there's nothing to receive? We have a receive timeout configured...

            logger.debug(f"Listening for HWC message")
            cmd = await read_command()
            logger.debug(f"Got HWC message: {' '.join(f'{b:02X}' for b in cmd)}")
            if cmd[0] in Reply.UPDATES:
                decode = decoders[cmd[0]]
                self._enqueue_update(decode(cmd))
            else:
                await self._replies_in.send(cmd)

    async def repeatable_command(self, cmd: List[int], reply_code: int=Reply.OK, tries: int=3) -> List[int]:
        for i in range(tries - 1):
            try:
                return await self.command(cmd, reply_code=reply_code)
            except TruncatedCommandError:
                pass
        return await self.command(cmd, reply_code=reply_code)

    async def command(self, cmd: List[int], reply_code: int=Reply.OK) -> List[int]:
        logger.debug(f"Send to HWC:     {' '.join(f'{b:02X}' for b in cmd)}")
        self.writer.write(bytes(cmd))
        reply = await self._replies_out.receive()

        if reply[0] in Reply.SUCCESS_REPLIES:
            assert reply[0] == reply_code
            return reply
        else:
            if reply[0] == Reply.UNKNOWN_OPCODE:
                self.controller.serial.flushOutput()
                await trio.sleep(0.02)
                self.controller.serial.flushInput()
                raise FailedCommandError("opcode unknown to the HWC; connection was reset")
            elif reply[0] == Reply.INVALID_OPCODE:
                raise FailedCommandError("opcode not supported")
            elif reply[0] == Reply.INVALID_PORT:
                raise FailedCommandError("port not supported or out of range")
            elif reply[0] == Reply.INVALID_CONFIG:
                raise FailedCommandError("sensor request invalid for output port")
            elif reply[0] == Reply.INVALID_MODE:
                raise FailedCommandError("unsupported motor mode")
            elif reply[0] == Reply.INVALID_FLAGS:
                raise FailedCommandError("unsupported combination of IO flags")
            elif reply[0] == Reply.INVALID_VALUE and cmd[0] == Command.MOTOR:
                raise FailedCommandError("unsupported motor power/velocity")
            elif reply[0] == Reply.INVALID_VALUE and cmd[0] == Command.SERVO:
                raise FailedCommandError("unsupported servo position")
            raise FailedCommandError("unknown hardware controller response")

    async def set_io_config(self, port, flags):
        await self.repeatable_command([Command.IO_CONFIG, port, flags])

    async def get_analog(self, port):
        _, port_, value_hi, value_lo = await self.repeatable_command([Command.ANALOG_REQ, port], Reply.ANALOG_REP)
        assert port_ == port
        return int.from_bytes([value_hi, value_lo], 'big')

    async def get_digital(self, port):
        _, port_, value = await self.repeatable_command([Command.DIGITAL_REQ, port], Reply.DIGITAL_REP)
        assert port_ == port
        assert value & ~0x01 == 0x00
        return value != 0

    async def set_motor(self, port, state, amount=0, reached_state=POWER, relative=None, absolute=None):
        if not -0x8000 < amount < 0x8000:
            raise FailedCommandError("unsupported motor power/velocity")
        value = amount if amount > 0 else (0x8000 | -amount)
        value_hi, value_lo = value.to_bytes(2, 'big')
        await self.repeatable_command([Command.MOTOR, port, state, value_hi, value_lo])

    async def set_servo(self, port, active, position):
        if not 0 <= position < 0x8000:
            raise FailedCommandError("unsupported servo position")
        value = position | (0x8000 if active else 0x0000)
        value_hi, value_lo = value.to_bytes(2, 'big')
        await self.repeatable_command([Command.SERVO, port, value_hi, value_lo])
