from hedgehog.protocol.messages import ack, vision

from . import CommandHandler, CommandRegistry


class VisionHandler(CommandHandler):
    _commands = CommandRegistry()

    @_commands.register(vision.CameraAction)
    async def camera_action(self, server, ident, msg):
        return ack.Acknowledgement()

    @_commands.register(vision.ReadFrameAction)
    async def read_frame_action(self, server, ident, msg):
        return ack.Acknowledgement()
