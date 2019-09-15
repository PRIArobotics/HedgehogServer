from contextlib import AsyncExitStack

import trio

from hedgehog.protocol.errors import FailedCommandError
from hedgehog.protocol.messages import ack, vision

from . import CommandHandler, CommandRegistry
from ..vision import open_camera, Grabber


class VisionHandler(CommandHandler):
    _commands = CommandRegistry()

    def __init__(self) -> None:
        super().__init__()
        self.open_count = 0
        self._stack = None  # AsyncExitStack
        self._grabber = None  # Grabber

    @_commands.register(vision.CameraAction)
    async def camera_action(self, server, ident, msg):
        if msg.open:
            if self.open_count == 0:
                self._stack = AsyncExitStack()
                cam = await trio.to_thread.run_sync(open_camera)
                self._grabber = Grabber(cam)
                await self._stack.enter_async_context(self._grabber)
            self.open_count += 1
        else:
            if self.open_count == 0:
                raise FailedCommandError("trying to close already closed camera")

            self.open_count -= 1
            if self.open_count == 0:
                await self._stack.aclose()
                self._grabber = None
                self._stack = None

        return ack.Acknowledgement()

    @_commands.register(vision.ReadFrameAction)
    async def read_frame_action(self, server, ident, msg):
        s, im = await self._grabber.aread()
        if not s:
            raise FailedCommandError("camera did not return an image")

        return ack.Acknowledgement()
