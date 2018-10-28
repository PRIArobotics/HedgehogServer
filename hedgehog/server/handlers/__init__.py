import functools
from typing import Awaitable, Callable, Dict, Type

from hedgehog.protocol import Header, Message
from hedgehog.utils import SimpleDecorator, Registry
from ..hedgehog_server import HedgehogServer


HandlerFunction = Callable[['CommandHandler', HedgehogServer, Header, Message], Awaitable[Message]]
HandlerCallback = Callable[[HedgehogServer, Header, Message], Awaitable[Message]]
HandlerCallbackDict = Dict[Type[Message], HandlerCallback]
HandlerDecorator = Callable[[Type[Message]], SimpleDecorator[HandlerFunction]]


CommandRegistry = Registry[Type[Message], HandlerFunction]


class CommandHandler(object):
    _commands = None  # type: CommandRegistry

    @property
    def handlers(self) -> HandlerCallbackDict:
        return {
            key: functools.partial(handler, self)
            for key, handler in self._commands.items()
        }


def merge(*handlers: CommandHandler) -> HandlerCallbackDict:
    result = {}  # type: HandlerCallbackDict
    for handler in handlers:
        dups = result.keys() & handler.handlers.keys()
        if len(dups) > 0:
            raise ValueError("Duplicate command handler for {}".format([dup.msg_name() for dup in dups]))
        result.update(handler.handlers)
    return result
