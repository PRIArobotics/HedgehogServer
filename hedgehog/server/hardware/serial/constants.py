class Command:
    # VERSION_REQ u8
    # --> VERSION_REP u8, uc_id u96, hw_version u8, sw_version u8
    VERSION_REQ = 0x01

    # EMERGENCY_ACTION u8, activate bool
    # --> OK u8
    EMERGENCY_ACTION = 0x05

    # EMERGENCY_REQ u8
    # --> EMERGENCY_REP u8, active bool
    EMERGENCY_REQ = 0x06

    # IO_CONFIG u8, port u8, on bool|pulldown bool|pullup bool|output bool
    # --> OK u8
    IO_CONFIG = 0x10

    # ANALOG_REQ u8, port u8
    # --> ANALOG_REP u8, port u8, value u16
    ANALOG_REQ = 0x20

    # IMU_RATE_REQ u8
    # --> IMU_RATE_REP u8, x s16, y s16, z s16
    IMU_RATE_REQ = 0x22

    # IMU_ACCEL_REQ u8
    # --> IMU_ACCEL_REP u8, x s16, y s16, z s16
    IMU_ACCEL_REQ = 0x23

    # IMU_POSE_REQ u8
    # --> IMU_POSE_REP u8, x s16, y s16, z s16
    IMU_POSE_REQ = 0x24

    # DIGITAL_REQ u8, port u8
    # --> DIGITAL_REP u8, port u8, value bool
    DIGITAL_REQ = 0x30

    # MOTOR u8, port u8, mode u8, amount s16
    # --> OK u8
    MOTOR = 0x40

    # MOTOR_POSITIONAL u8, port u8, mode u8, amount s16, relative bool|done_mode u7, position s32
    # --> (status codes)
    MOTOR_POSITIONAL = 0x41

    # MOTOR_SERVO u8, port u8, max_velocity u16, relative bool, position s32
    # --> (status codes)
    MOTOR_SERVO = 0x42

    # MOTOR_CONFIG_DC u8, port u8
    # --> OK u8
    MOTOR_CONFIG_DC = 0x48

    # MOTOR_CONFIG_ENCODER u8, port u8, encoder_a_port u8, encoder_b_port u8
    # --> OK u8
    MOTOR_CONFIG_ENCODER = 0x49

    # MOTOR_CONFIG_STEPPER u8, port u8
    # --> OK u8
    MOTOR_CONFIG_STEPPER = 0x4A

    # SERVO u8, port u8, active bool|value u15
    # --> OK u8
    SERVO = 0x50

    # UART u8, length u8, data u8{length}
    # --> OK u8
    UART = 0x60

    # SPEAKER u8, frequency u16
    # --> OK u8
    SPEAKER = 0x70


class Reply:
    VERSION_REP = 0x02
    SHUTDOWN = 0x03
    EMERGENCY_STOP = 0x04
    EMERGENCY_REP = 0x05
    OK = 0x80
    UNKNOWN_OPCODE = 0x81
    INVALID_OPCODE = 0x82
    INVALID_PORT = 0x83
    INVALID_CONFIG = 0x84
    INVALID_MODE = 0x85
    INVALID_FLAGS = 0x86
    INVALID_VALUE = 0x87
    ANALOG_REP = 0xA1
    IMU_RATE_REP = 0xA2
    IMU_ACCEL_REP = 0xA3
    IMU_POSE_REP = 0xA4
    DIGITAL_REP = 0xB1
    MOTOR_DONE_UPDATE = 0xC3
    UART_UPDATE = 0xE1

    # these are received for specific commands, meaning that it succeeded
    # possibly followed by a payload
    SUCCESS_REPLIES = {
        VERSION_REP,
        EMERGENCY_REP,
        OK,
        ANALOG_REP,
        IMU_RATE_REP,
        IMU_ACCEL_REP,
        IMU_POSE_REP,
        DIGITAL_REP,
    }

    # these may be received for any command, meaning that it failed
    ERROR_REPLIES = {
        UNKNOWN_OPCODE,
        INVALID_OPCODE,
        INVALID_PORT,
        INVALID_CONFIG,
        INVALID_MODE,
        INVALID_FLAGS,
        INVALID_VALUE,
    }

    # these may be received at any time and aren't responses to previous commands
    # possibly followed by a payload
    UPDATES = {
        # (asynchronous)
        # --> SHUTDOWN u8
        SHUTDOWN,

        # (asynchronous)
        # --> EMERGENCY_STOP u8
        EMERGENCY_STOP,

        # (asynchronous)
        # --> MOTOR_DONE_UPDATE u8, port u8
        MOTOR_DONE_UPDATE,

        # (asynchronous)
        # --> UART_UPDATE u8, length u8, data u8{length}
        UART_UPDATE,
    }

    # the length of replies where it is known
    # if unknown, the first byte after the reply code must contain the length of the payload in bytes
    # that means, the complete message has length+2 bytes: the reply code, the length, and the payload
    LENGTHS = {
        VERSION_REP: 15,
        SHUTDOWN: 1,
        EMERGENCY_STOP: 1,
        EMERGENCY_REP: 2,
        OK: 1,
        UNKNOWN_OPCODE: 1,
        INVALID_OPCODE: 1,
        INVALID_PORT: 1,
        INVALID_CONFIG: 1,
        INVALID_MODE: 1,
        INVALID_FLAGS: 1,
        INVALID_VALUE: 1,
        ANALOG_REP: 4,
        IMU_RATE_REP: 7,
        IMU_ACCEL_REP: 7,
        IMU_POSE_REP: 7,
        DIGITAL_REP: 3,
        MOTOR_DONE_UPDATE: 2,
        # UART_UPDATE: variable length
    }


# number analog and digital ports together, servo and motor ports separately

class SpecialAnalog:
    BATTERY_VOLTAGE = 0x80


class SpecialDigital:
    # output only
    LED0 = 0x90
    LED1 = 0x91


class MotorMode:
    # power: use constant duty cycle
    POWER = 0x00
    # brake: short-circuit with constant duty cycle
    BRAKE = 0x01
    # velocity: constant speed with closed-circuit control; encoder motors only (for now)
    VELOCITY = 0x02


class MotorDoneMode:
    # what does a positional command do after reaching the target position?
    # behavior in terms of a regular motor command
    # off: power = 0
    OFF = 0x00
    # brake: brake = 1000
    BRAKE = 0x01
    # active_brake: velocity = 0
    ACTIVE_BRAKE = 0x02
