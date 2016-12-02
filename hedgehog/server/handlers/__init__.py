def command_handlers():
    _handlers = {}

    def command(msg):
        def decorator(func):
            _handlers[msg.meta.discriminator] = func
            return func
        return decorator
    return _handlers, command


def to_dict(*handlers):
    result = {}
    for handler in handlers:
        dups = result.keys() & handler.handlers.keys()
        if len(dups) > 0:
            raise ValueError("Duplicate command handler for {}".format(dups))
        result.update(handler.handlers)
    return result


class CommandHandler(object):
    _handlers = None

    def __init__(self):
        self.handlers = {
            key: handler.__get__(self)
            for key, handler in self._handlers.items()
        }
