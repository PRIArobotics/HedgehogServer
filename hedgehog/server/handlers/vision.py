from typing import Callable, Dict, DefaultDict, Generic, Optional, Set, TypeVar

from collections import defaultdict
from dataclasses import dataclass
from functools import partial

import trio
import cv2
import numpy as np

from hedgehog.protocol import Header
from hedgehog.protocol.errors import FailedCommandError
from hedgehog.protocol.messages import ack, vision as vision_msg

from . import CommandHandler, CommandRegistry
from .. import vision


T = TypeVar('T')


def pack_faces(feature) -> vision_msg.Feature:
    return vision_msg.FacesFeature([
        vision_msg.Face(rect)
        for rect in feature
    ])


def pack_blobs(feature) -> vision_msg.Feature:
    blobs, _ = feature
    return vision_msg.BlobsFeature([
        vision_msg.Blob(rect, centroid, confidence)
        for rect, centroid, confidence in blobs
    ])


@dataclass
class Channel(Generic[T]):
    msg: vision_msg.Channel
    detect: Callable[[np.ndarray], T]
    highlight: Callable[[np.ndarray, T], np.ndarray]
    pack: Callable[[T], vision_msg.Feature]


class ChannelData(Generic[T]):
    feature: Optional[T] = None
    highlight: Optional[np.ndarray] = None
    packed: Optional[vision_msg.Feature] = None

    def get_feature(self, img: np.ndarray, channel: Channel[T]):
        if self.feature is None:
            self.feature = channel.detect(img)
        return self.feature

    def get_highlight(self, img: np.ndarray, channel: Channel[T]):
        if self.highlight is None:
            feature = self.get_feature(img, channel)
            self.highlight = channel.highlight(img, feature)
        return self.highlight

    def get_packed(self, img: np.ndarray, channel: Channel[T]):
        if self.packed is None:
            feature = self.get_feature(img, channel)
            self.packed = channel.pack(feature)
        return self.packed


class Connection:
    channels: Dict[str, Channel]
    channel_data: DefaultDict[str, ChannelData]

    def __init__(self):
        self.channels = {}
        self.channel_data = defaultdict(ChannelData)

    def set_channels(self, channels: Dict[str, vision_msg.Channel]):
        for key, channel in channels.items():
            if isinstance(channel, vision_msg.FacesChannel):
                self.channels[key] = Channel(
                    channel,
                    partial(vision.detect_faces, vision.haar_face_cascade),
                    vision.highlight_faces,
                    pack_faces,
                )
            elif isinstance(channel, vision_msg.BlobsChannel):
                self.channels[key] = Channel(
                    channel,
                    partial(vision.detect_blobs, min_hsv=channel.hsv_min, max_hsv=channel.hsv_max),
                    vision.highlight_blobs,
                    pack_blobs,
                )
            else:  # pragma: nocover
                assert False


@dataclass
class Camera:
    grabber: vision.Grabber
    scope: trio.CancelScope
    closed: trio.Event


class VisionHandler(CommandHandler):
    _commands = CommandRegistry()

    _connections: Dict[Header, Connection] = {}
    _camera: Optional[Camera] = None

    _img: Optional[np.ndarray] = None

    def __init__(self) -> None:
        self._connections = {}

    async def _handle_camera(self, *, task_status=trio.TASK_STATUS_IGNORED):
        try:
            async with trio.open_nursery() as nursery:
                grabber = vision.Grabber(vision.camera())
                await nursery.start(grabber.run)
                self._camera = Camera(grabber, nursery.cancel_scope, trio.Event())
                task_status.started()
        finally:
            self._camera.closed.set()
            self._connections.clear()
            self._camera = None
            self._img = None

    async def _close_camera(self):
        self._camera.scope.cancel()
        await self._camera.closed.wait()

    @_commands.register(vision_msg.OpenCameraAction)
    async def open_camera_action(self, server, ident, msg):
        if ident in self._connections:
            raise FailedCommandError("trying to open already opened camera")

        if not self._connections:
            await server.add_task(self._handle_camera)

        self._connections[ident] = Connection()

        return ack.Acknowledgement()

    @_commands.register(vision_msg.CloseCameraAction)
    async def close_camera_action(self, server, ident, msg):
        if ident not in self._connections:
            # don't fail on closing if we're not open
            return ack.Acknowledgement()

        del self._connections[ident]

        if not self._connections and self._camera:
            await self._close_camera()

        return ack.Acknowledgement()

    @_commands.register(vision_msg.CreateChannelAction)
    async def create_channel_action(self, server, ident, msg):
        try:
            conn = self._connections[ident]
        except KeyError:
            raise FailedCommandError("camera is closed")
        else:
            dups = set(msg.channels) & set(conn.channels)
            if dups:
                raise FailedCommandError(f"duplicate keys: {', '.join(dups)}")

            conn.set_channels(msg.channels)
            return ack.Acknowledgement()

    @_commands.register(vision_msg.UpdateChannelAction)
    async def update_channel_action(self, server, ident, msg):
        try:
            conn = self._connections[ident]
        except KeyError:
            raise FailedCommandError("camera is closed")
        else:
            missing = set(msg.channels) - set(conn.channels)
            if missing:
                raise FailedCommandError(f"nonexistent keys: {', '.join(missing)}")

            conn.set_channels(msg.channels)
            return ack.Acknowledgement()

    @_commands.register(vision_msg.DeleteChannelAction)
    async def delete_channel_action(self, server, ident, msg):
        try:
            conn = self._connections[ident]
        except KeyError:
            raise FailedCommandError("camera is closed")
        else:
            missing = msg.keys - set(conn.channels)
            if missing:
                raise FailedCommandError(f"nonexistent keys: {', '.join(missing)}")

            for key in msg.keys:
                del conn.channels[key]
            return ack.Acknowledgement()

    @_commands.register(vision_msg.ChannelRequest)
    async def channel_request(self, server, ident, msg):
        try:
            conn = self._connections[ident]
        except KeyError:
            raise FailedCommandError("camera is closed")
        else:
            missing = msg.keys - set(conn.channels)
            if missing:
                raise FailedCommandError(f"nonexistent keys: {', '.join(missing)}")

            if msg.keys:
                return vision_msg.ChannelReply({key: conn.channels[key].msg for key in msg.keys})
            else:
                return vision_msg.ChannelReply({key: channel.msg for key, channel in conn.channels.items()})

    @_commands.register(vision_msg.CaptureFrameAction)
    async def capture_frame_action(self, server, ident, msg):
        if ident not in self._connections:
            raise FailedCommandError("camera is closed")

        img = await self._camera.grabber.read()
        if img is None:
            self._img = None
            self._channel_data = None
            raise FailedCommandError("camera did not return an image")

        self._img = img
        for conn in self._connections.values():
            conn.channel_data.clear()

        return ack.Acknowledgement()

    @_commands.register(vision_msg.FrameRequest)
    async def frame_request(self, server, ident, msg):
        try:
            conn = self._connections[ident]
        except KeyError:
            raise FailedCommandError("camera is closed")
        else:
            if self._img is None:
                raise FailedCommandError("no frame available")

            if msg.highlight is None:
                img = self._img
            elif msg.highlight in conn.channels:
                channel_data = conn.channel_data[msg.highlight]
                img = channel_data.get_highlight(self._img, conn.channels[msg.highlight])
            else:
                raise FailedCommandError(f"no such channel: {msg.highlight}")

            s, img = cv2.imencode('.jpg', img)
            if not s:
                raise FailedCommandError("encoding image failed")

            return vision_msg.FrameReply(msg.highlight, img.tostring())

    @_commands.register(vision_msg.FeatureRequest)
    async def feature_request(self, server, ident, msg):
        try:
            conn = self._connections[ident]
        except KeyError:
            raise FailedCommandError("camera is closed")
        else:
            if self._img is None:
                raise FailedCommandError("no frame available")

            if msg.channel in conn.channels:
                channel_data = conn.channel_data[msg.channel]
                packed = channel_data.get_packed(self._img, conn.channels[msg.channel])
                return vision_msg.FeatureReply(msg.channel, packed)
            else:
                raise FailedCommandError(f"no such channel: {msg.channel}")
