from . import HardwareAdapter, POWER


def logged(func):
    def decorated(self, *args, **kwargs):
        name = func.__name__
        result = None
        try:
            result = getattr(self.delegate, name)(*args, **kwargs)
            return result
        except BaseException as ex:
            result = ex
            raise
        finally:
            args_str = ", ".join((repr(arg) for arg in args))
            kwargs_str = ", ".join(("{}={}".format(str(k), repr(v)) for k, v in kwargs.items()))

            if len(args) > 0 and len(kwargs) > 0:
                arg_str = args_str + ", " + kwargs_str
            else:
                arg_str = args_str + kwargs_str

            print("{}({}) = {}".format(name, arg_str, result))
    return decorated


class LoggingHardwareAdapter(HardwareAdapter):
    def __init__(self, delegate):
        super().__init__()
        self.delegate = delegate

    @logged
    def set_io_state(self, *args, **kwargs): pass

    @logged
    def get_analog(self, *args, **kwargs): pass

    @logged
    def get_digital(self, *args, **kwargs): pass

    @logged
    def set_motor(self, *args, **kwargs): pass

    @logged
    def get_motor(self, *args, **kwargs): pass

    @logged
    def set_motor_position(self, *args, **kwargs): pass

    @logged
    def set_servo(self, *args, **kwargs): pass
