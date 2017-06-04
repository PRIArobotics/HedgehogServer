from typing import Any, Callable, Dict, Tuple, Type

from hedgehog.protocol import Header, Message
from ..hedgehog_server import HedgehogServerActor


HandlerFunction = Callable[['CommandHandler', HedgehogServerActor, Header, Message], Message]
HandlerCallback = Callable[[HedgehogServerActor, Header, Message], Message]
HandlerCallbackDict = Dict[Type[Message], HandlerCallback]
HandlerDecorator = Callable[[Type[Message]], Callable[[HandlerFunction], HandlerFunction]]


def command_handlers() -> Tuple[Dict[Type[Message], HandlerFunction], HandlerDecorator]:
    _handlers = {}  # type: Dict[Type[Message], HandlerFunction]

    def command(msg: Type[Message]):
        def decorator(func: HandlerFunction):
            _handlers[msg] = func
            return func
        return decorator
    return _handlers, command


class CommandHandler(object):
    _handlers = None  # type: Dict[Type[Message], HandlerFunction]

    def __init__(self) -> None:
        self.handlers = {
            key: handler.__get__(self)
            for key, handler in self._handlers.items()
        }  # type: Dict[Type[Message], HandlerCallback]


def to_dict(*handlers: CommandHandler) -> HandlerCallbackDict:
    result = {}  # type: HandlerCallbackDict
    for handler in handlers:
        dups = result.keys() & handler.handlers.keys()
        if len(dups) > 0:
            raise ValueError("Duplicate command handler for {}".format([dup.msg_name() for dup in dups]))
        result.update(handler.handlers)
    return result
