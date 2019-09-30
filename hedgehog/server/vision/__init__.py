import logging
import cv2
import numpy as np
import os
import trio

from contextlib import contextmanager
from functools import partial
from threading import Thread

logger = logging.getLogger(__name__)


class Grabber:
    def __init__(self, cap_ctx):
        # context manager that opens and closes an cv2.VideoCapture object
        self._cap_ctx = cap_ctx

        # ## capture thread management
        # stop flag for cancellation
        self._stop = False

        # ## frame synchronization
        # event for waiting for next frame
        self._event = trio.Event()
        # next available frame
        self._frame = None

    async def run(self, *, task_status=trio.TASK_STATUS_IGNORED):
        try:
            Thread(target=partial(self._run, trio.hazmat.current_trio_token())).start()
            task_status.started()
            await trio.sleep_forever()
        finally:
            self._stop = True
            with trio.CancelScope(shield=True):
                # wait for the background thread to finish
                while await self.read() is not None:
                    logger.info("wait")
                logger.info("done")

    def _run(self, trio_token):
        logger.info("Grabber thread started")
        try:
            with self._cap_ctx as cap:
                s = True
                while s and not self._stop:
                    s, frame = cap.read()
                    trio.from_thread.run(self._publish, frame, trio_token=trio_token)
            trio.from_thread.run(self._publish, None, trio_token=trio_token)
        finally:
            logger.info("Grabber thread finished")

    async def _publish(self, frame):
        self._frame = frame
        self._event.set()

    async def read(self):
        await self._event.wait()
        if self._frame is not None:
            # there will be further frames
            # as we're on the event loop thread,
            # there can come no race condition from replacing the Event
            self._event = trio.Event()
        return self._frame


@contextmanager
def camera():
    cam = cv2.VideoCapture(0)
    cam.set(cv2.CAP_PROP_FRAME_WIDTH, 160)
    cam.set(cv2.CAP_PROP_FRAME_HEIGHT, 120)
    yield cam
    cam.release()


haar_face_cascade = cv2.CascadeClassifier(
    os.path.join(os.path.dirname(__file__), 'haarcascade_frontalface_alt.xml')
)


def detect_faces(f_cascade, img, scaleFactor=1.1, minNeighbors=5):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return f_cascade.detectMultiScale(gray, scaleFactor=scaleFactor, minNeighbors=minNeighbors)


def highlight_faces(img, faces):
    img = np.copy(img)
    for (x, y, w, h) in faces:
        cv2.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), 2)
    return img


def detect_blobs(img, min_hsv, max_hsv):
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV_FULL)
    if min_hsv[0] <= max_hsv[0]:
        mask = cv2.inRange(hsv, min_hsv, max_hsv)
    else:
        mask = (
                cv2.inRange(hsv, (0, *min_hsv[1:3]), max_hsv) |
                cv2.inRange(hsv, min_hsv, (255, *max_hsv[1:3]))
        )
    mask = mask.reshape((*mask.shape, 1))

    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_TC89_L1)

    blobs = []
    for c in contours:
        rect = cv2.boundingRect(c)
        _, _, w, h = rect
        if w < 3 or h < 3:
            continue

        m = cv2.moments(c)
        m00, m10, m01 = (m[x] for x in ('m00', 'm10', 'm01'))
        if m00 == 0:
            continue
        confidence = m00 / (w*h)
        centroid = int(m10 / m00), int(m01 / m00)

        blobs.append((rect, centroid, confidence))

    def area(blob):
        rect, _, _ = blob
        _, _, w, h = rect
        return w*h

    blobs.sort(key=area, reverse=True)
    return blobs, mask


def highlight_blobs(img, blobs_data):
    blobs, mask = blobs_data
    img = np.copy(img)
    for (x, y, w, h), _, _ in blobs:
        img |= mask
        cv2.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), 2)
    return img
