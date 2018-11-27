import base64
import copy
import time
import json
import socket
import uuid

from pynatsio import exceptions as exc


_HOST = socket.gethostbyname(socket.getfqdn())


class Message:
    """Message class that is returned as part of request() and subscribe() functions.

    """

    def __init__(self, subject: str="", reply: str="", data=None, **kwargs):
        self._subject = subject
        self._reply = reply
        self._data = data
        self._metadata = kwargs

    @property
    def subject(self) -> str:
        """Return the subject (queue / key) on this message

        :return: str

        """
        return self._subject

    @property
    def data(self):
        """Return data written by message author. This can be anything that JSON can handle.

        :return: ?

        """
        return copy.copy(self._data)

    @property
    def reply(self) -> str:
        """Return the reply address (if set) on this message

        :return: str

        """
        return self._reply

    def encode(self) -> bytes:
        """Encode this message into bytes

        :return: bytes

        """
        json_data = {
            "data": self._data,
            "metadata": self.metadata,
        }
        return base64.b64encode(bytes(json.dumps(json_data), encoding='utf8'))

    @property
    def metadata(self) -> dict:
        """Return the metadata associated with this message

        :return: dict

        """
        if self._metadata:
            return self._metadata

        return {
            "host": _HOST,
            "id": str(uuid.uuid4()),
            "time": time.time(),
        }

    @classmethod
    def decode(cls, subject: str, reply: str, raw: bytes):
        """Decode a message from it's constituent parts.

        :param subject: message subject (key / queue)
        :param reply: message reply address, if set
        :param raw: raw bytes of the message data (that is, the payload)
        :return: Message

        """
        try:
            msg_data = json.loads(base64.b64decode(raw))
        except Exception as e:
            raise exc.MessageFormatError(e)

        me = cls(subject, reply, data=msg_data.get("data"), **msg_data.get("metadata", {}))
        return me
