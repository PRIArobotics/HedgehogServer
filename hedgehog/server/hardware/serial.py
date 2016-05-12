import serial
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
INVALID_PORT = 0x81
INVALID_IO = 0x82
INVALID_MODE = 0x83
INVALID_FLAGS = 0x84
INVALID_VALUE = 0x85
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
    INVALID_PORT,
    INVALID_IO,
    INVALID_MODE,
    INVALID_FLAGS,
    INVALID_VALUE,
}
_cmd_lengths = {
    OK: 1,
    INVALID_PORT: 1,
    INVALID_IO: 1,
    INVALID_MODE: 1,
    INVALID_FLAGS: 1,
    INVALID_VALUE: 1,
    ANALOG_REP: 4,
    DIGITAL_REP: 3,
}

class SerialHardwareAdapter(HardwareAdapter):
    def __init__(self, motor_state_update_cb=None):
        super().__init__(motor_state_update_cb=motor_state_update_cb)
        self.serial = serial.Serial(
            port='/dev/ttyS3',
            baudrate=115200,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_EVEN,
            stopbits=serial.STOPBITS_ONE,
            timeout=5,
            xonxoff=False,
            rtscts=False,
            writeTimeout=None,
            dsrdtr=False,
            interCharTimeout=None,
        )

    def command(self, cmd, reply_code=OK):
        def read_command():
            cmd = self.serial.read()
            if cmd[0] == SERIAL_UPDATE:
                cmd += self.serial.read(2)
                cmd += self.serial.read(cmd[1])
            else:
                length = _cmd_lengths[cmd[0]]
                if length > 1:
                    cmd += self.serial.read(length - 1)
            return list(cmd)

        self.serial.write(bytes(cmd))
        reply = read_command()
        while reply[0] not in _replies | _errors:
            # TODO do something with the update
            reply = read_command()

        if reply[0] in _replies:
            assert reply[0] == reply_code
            return reply
        else:
            if reply[0] == INVALID_PORT:
                raise FailedCommandError("port not supported or out of range")
            elif reply[0] == INVALID_IO and cmd[0] == ANALOG_REQ:
                raise FailedCommandError("analog sensor request invalid for digital or output port")
            elif reply[0] == INVALID_IO and cmd[0] == DIGITAL_REQ:
                raise FailedCommandError("digital sensor request invalid for analog or output port")
            elif reply[0] == INVALID_MODE:
                raise FailedCommandError("unsupported motor mode")
            elif reply[0] == INVALID_FLAGS:
                raise FailedCommandError("unsupported combination of IO flags")
            elif reply[0] == INVALID_VALUE and cmd[0] == MOTOR:
                raise FailedCommandError("unsupported motor power/velocity")
            elif reply[0] == INVALID_VALUE and cmd[0] == SERVO:
                raise FailedCommandError("unsupported servo position")

    def set_io_state(self, port, flags):
        self.command([IO_STATE, port, flags])

    def get_analog(self, port):
        _, port_, value_hi, value_lo = self.command([ANALOG_REQ, port], ANALOG_REP)
        assert port_ == port
        return int.from_bytes([value_hi, value_lo], 'big')

    def get_digital(self, port):
        _, port_, value = self.command([DIGITAL_REQ, port], DIGITAL_REP)
        assert port_ == port
        assert value & ~0x01 == 0x00
        return value != 0

    def set_motor(self, port, state, amount=0, reached_state=POWER, relative=None, absolute=None):
        if not -0x8000 < amount < 0x8000:
            raise FailedCommandError("unsupported motor power/velocity")
        value = amount if amount > 0 else (0x8000 | -amount)
        value_hi, value_lo = value.to_bytes(2, 'big')
        self.command([MOTOR, port, state, value_hi, value_lo])

    def set_servo(self, port, active, position):
        if not 0 <= position < 0x8000:
            raise FailedCommandError("unsupported servo position")
        value = position | (0x8000 if active else 0x0000)
        value_hi, value_lo = value.to_bytes(2, 'big')
        self.command([MOTOR, port, value_hi, value_lo])

