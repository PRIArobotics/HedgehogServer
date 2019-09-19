import logging
import cv2
import trio

from threading import Condition, Thread

logger = logging.getLogger(__name__)


class Grabber:
    """
    Grabber ensures that camera frames are grabbed as soon as they arrive, so that the latest frame is always processed.
    To do this, Grabber starts a background thread for reading the camera.
    Grabber manages that thread as a context manager.
    Only when the application wants to process a frame, that frame is retrieved using the `read` method.
    `read` will always return the latest data fetched from the capture device,
    even if that data indicated there are no frames left.

    Grabber offers an async interface for trio as well, with an async context manager and the `aread` method.
    """

    _EMPTY = 0
    _GRABBED = 1
    _CLOSED = 2

    def __init__(self, cap):
        self._cap = cap
        self._state = Grabber._EMPTY
        self._cond = Condition()

    def __enter__(self):
        Thread(target=self._grabber).start()
        return self

    async def __aenter__(self):
        self.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        with self._cond:
            self._state = Grabber._CLOSED

    async def __aexit__(self, exc_type, exc_value, traceback):
        await trio.to_thread.run_sync(self.__exit__, exc_type, exc_value, traceback)

    def _grabber(self):
        logger.info("Grabber thread started")
        while True:
            with self._cond:
                if self._state == Grabber._CLOSED:
                    break
                elif self._cap.grab():
                    # set a flag that a frame may be retrieved
                    self._state = Grabber._GRABBED
                    self._cond.notify_all()
                else:
                    # set a flag that EOF was reached
                    # grab() has probably invalidated the previous frame
                    # that's ok; it's old by now and we don't want it
                    self._state = Grabber._CLOSED
                    self._cond.notify_all()
                    break
        self._cap.release()
        logger.info("Grabber thread finished")

    def read(self):
        with self._cond:
            self._cond.wait_for(lambda: self._state != Grabber._EMPTY)
            if self._state == Grabber._GRABBED:
                # decode and retrieve the latest grabbed frame
                self._state = Grabber._EMPTY
                return self._cap.retrieve()
            else:
                # grabber was closed
                return False, None

    async def aread(self):
        return await trio.to_thread.run_sync(self.read)


def open_camera():
    cam = cv2.VideoCapture(0)
    cam.set(cv2.CAP_PROP_FRAME_WIDTH, 160)
    cam.set(cv2.CAP_PROP_FRAME_HEIGHT, 120)
    return cam


haar_face_cascade = cv2.CascadeClassifier('hedgehog/server/vision/haarcascade_frontalface_alt.xml')


def detect_faces(f_cascade, img, scaleFactor=1.1, minNeighbors=5):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    return f_cascade.detectMultiScale(gray, scaleFactor=scaleFactor, minNeighbors=minNeighbors)


def highlight_faces(img, faces):
    for (x, y, w, h) in faces:
        cv2.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), 2)
    return img


def detect_blobs(img, min_hsv, max_hsv):
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV_FULL)
    mask = cv2.inRange(hsv, min_hsv, max_hsv)
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
    return blobs


def highlight_blobs(img, blobs):
    for (x, y, w, h), _, _ in blobs:
        cv2.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), 2)
    return img
