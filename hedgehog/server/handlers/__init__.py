from typing import Any, Callable, Dict, Tuple, Type

from hedgehog.protocol.messages import Message


HandlerFunction = Callable[[Any, Any, Any, Any], Message]
HandlerCallback = Callable[[Any, Any, Any], Message]
HandlerCallbackDict = Dict[str, HandlerCallback]
HandlerDecorator = Callable[[Type[Message]], Callable[[HandlerFunction], HandlerFunction]]


def command_handlers() -> Tuple[Dict[str, HandlerFunction], HandlerDecorator]:
    _handlers = {}  # type: Dict[str, HandlerFunction]

    def command(msg: Type[Message]):
        def decorator(func: HandlerFunction):
            _handlers[msg.meta.discriminator] = func
            return func
        return decorator
    return _handlers, command


class CommandHandler(object):
    _handlers = None  # type: Dict[str, HandlerFunction]

    def __init__(self) -> None:
        self.handlers = {
            key: handler.__get__(self)
            for key, handler in self._handlers.items()
        }  # type: Dict[str, HandlerCallback]


def to_dict(*handlers: CommandHandler) -> Dict[str, HandlerCallback]:
    result = {}  # type: HandlerCallbackDict
    for handler in handlers:
        dups = result.keys() & handler.handlers.keys()
        if len(dups) > 0:
            raise ValueError("Duplicate command handler for {}".format(dups))
        result.update(handler.handlers)
    return result
