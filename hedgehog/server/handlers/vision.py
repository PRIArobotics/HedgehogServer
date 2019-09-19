from typing import Callable, Dict, DefaultDict, Generic, Optional, TypeVar

from collections import defaultdict
from contextlib import AsyncExitStack
from dataclasses import dataclass
from functools import partial

import trio
import cv2
import numpy as np

from hedgehog.protocol.errors import FailedCommandError
from hedgehog.protocol.messages import ack, vision as vision_msg

from . import CommandHandler, CommandRegistry
from .. import vision


T = TypeVar('T')


@dataclass
class Channel(Generic[T]):
    msg: vision_msg.Channel
    detect: Callable[[np.ndarray], T]
    highlight: Callable[[np.ndarray, T], np.ndarray]


class ChannelData(Generic[T]):
    result: Optional[T] = None
    highlight: Optional[np.ndarray] = None

    def get_result(self, img: np.ndarray, channel: Channel[T]):
        if self.result is None:
            self.result = channel.detect(img)
        return self.result

    def get_highlight(self, img: np.ndarray, channel: Channel[T]):
        if self.highlight is None:
            result = self.get_result(img, channel)
            self.highlight = channel.highlight(img, result)
        return self.highlight


class VisionHandler(CommandHandler):
    _commands = CommandRegistry()

    open_count: int = 0
    _stack: Optional[AsyncExitStack] = None
    _grabber: Optional[vision.Grabber] = None
    _channels: Dict[str, Channel]
    _img: Optional[np.ndarray] = None
    _channel_data: Optional[DefaultDict[str, ChannelData]] = None

    def __init__(self) -> None:
        self._channels = {}

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
            self._channel_data = None

        return ack.Acknowledgement()

    def set_channels(self, channels: Dict[str, vision_msg.Channel]):
        for key, channel in channels.items():
            if isinstance(channel, vision_msg.FacesChannel):
                self._channels[key] = Channel(
                    channel,
                    partial(vision.detect_faces, vision.haar_face_cascade),
                    vision.highlight_faces
                )
            elif isinstance(channel, vision_msg.BlobsChannel):
                self._channels[key] = Channel(
                    channel,
                    partial(vision.detect_blobs, min_hsv=channel.hsv_min, max_hsv=channel.hsv_max),
                    vision.highlight_blobs
                )
            else:  # pragma: nocover
                assert False

    @_commands.register(vision_msg.CreateChannelAction)
    async def create_channel_action(self, server, ident, msg):
        dups = set(msg.channels) & set(self._channels)
        if dups:
            raise FailedCommandError(f"duplicate keys: {', '.join(dups)}")

        self.set_channels(msg.channels)
        return ack.Acknowledgement()

    @_commands.register(vision_msg.UpdateChannelAction)
    async def update_channel_action(self, server, ident, msg):
        missing = set(msg.channels) - set(self._channels)
        if missing:
            raise FailedCommandError(f"nonexistent keys: {', '.join(missing)}")

        self.set_channels(msg.channels)
        return ack.Acknowledgement()

    @_commands.register(vision_msg.DeleteChannelAction)
    async def delete_channel_action(self, server, ident, msg):
        missing = msg.keys - set(self._channels)
        if missing:
            raise FailedCommandError(f"nonexistent keys: {', '.join(missing)}")

        for key in msg.keys:
            del self._channels[key]
        return ack.Acknowledgement()

    @_commands.register(vision_msg.ChannelRequest)
    async def channel_request(self, server, ident, msg):
        missing = msg.keys - set(self._channels)
        if missing:
            raise FailedCommandError(f"nonexistent keys: {', '.join(missing)}")

        if msg.keys:
            return vision_msg.ChannelReply({key: self._channels[key].msg for key in msg.keys})
        else:
            return vision_msg.ChannelReply({key: channel.msg for key, channel in self._channels.items()})

    @_commands.register(vision_msg.CaptureFrameAction)
    async def capture_frame_action(self, server, ident, msg):
        if self._grabber is None:
            raise FailedCommandError("camera is closed")

        s, img = await self._grabber.aread()
        if not s:
            self._img = None
            self._channel_data = None
            raise FailedCommandError("camera did not return an image")

        self._img = img
        self._channel_data = defaultdict(ChannelData)

        return ack.Acknowledgement()

    @_commands.register(vision_msg.FrameRequest)
    async def frame_request(self, server, ident, msg):
        if self._img is None:
            raise FailedCommandError("no frame available")

        if msg.highlight is None:
            img = self._img
        elif msg.highlight in self._channels:
            channel_data = self._channel_data[msg.highlight]
            img = channel_data.get_highlight(self._img, self._channels[msg.highlight])
        else:
            raise FailedCommandError(f"no such channel: {msg.highlight}")

        s, img = cv2.imencode('.jpg', img)
        if not s:
            raise FailedCommandError("encoding image failed")

        return vision_msg.FrameReply(msg.highlight, img.tostring())
