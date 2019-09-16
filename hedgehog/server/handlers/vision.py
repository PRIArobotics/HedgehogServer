from contextlib import AsyncExitStack

import trio
import cv2

from hedgehog.protocol.errors import FailedCommandError
from hedgehog.protocol.messages import ack, vision as vision_msg

from . import CommandHandler, CommandRegistry
from .. import vision


class VisionHandler(CommandHandler):
    _commands = CommandRegistry()

    def __init__(self) -> None:
        super().__init__()
        self.open_count = 0
        self._stack = None  # AsyncExitStack
        self._grabber = None  # Grabber
        self._img = None

    @_commands.register(vision_msg.OpenCameraAction)
    async def open_camera_action(self, server, ident, msg):
        if self.open_count == 0:
            self._stack = AsyncExitStack()
            cam = await trio.to_thread.run_sync(vision.open_camera)
            self._grabber = vision.Grabber(cam)
            await self._stack.enter_async_context(self._grabber)
        self.open_count += 1

        return ack.Acknowledgement()

    @_commands.register(vision_msg.CloseCameraAction)
    async def close_camera_action(self, server, ident, msg):
        if self.open_count == 0:
            raise FailedCommandError("trying to close already closed camera")

        self.open_count -= 1
        if self.open_count == 0:
            await self._stack.aclose()
            self._grabber = None
            self._stack = None
            self._img = None

        return ack.Acknowledgement()

    @_commands.register(vision_msg.RetrieveFrameAction)
    async def retrieve_frame_action(self, server, ident, msg):
        if self._grabber is None:
            raise FailedCommandError("camer is closed")

        s, img = await self._grabber.aread()
        if not s:
            self._img = None
            raise FailedCommandError("camera did not return an image")

        self._img = img

        return ack.Acknowledgement()

    @_commands.register(vision_msg.FrameRequest)
    async def frame_request(self, server, ident, msg):
        if self._img is None:
            raise FailedCommandError("no frame available")

        s, img = cv2.imencode('.jpg', self._img)
        if not s:
            raise FailedCommandError("encoding image failed")

        return vision_msg.FrameReply(msg.highlight, img.tostring())
