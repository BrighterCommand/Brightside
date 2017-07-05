""""
File             : messaging.py
Author           : ian
Created          : 07-08-2016

Last Modified By : ian
Last Modified On : 07-08-2016
***********************************************************************
The MIT License (MIT)
Copyright © 2016 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the “Software”), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
***********************************************************************
"""

from uuid import UUID, uuid4
from abc import ABCMeta, abstractmethod
from enum import Enum, unique
from multiprocessing import Queue


class BrightsideMessageBodyType:
    application_json = "application/json"
    application_xml = "application/xml"
    text_plain = "text/plain"
    text_xml = "text/xml"


class BrightsideMessageBody:
    """The body of our message. Note that this must use the same binary payload approach as Paramore Brighter to
        ensure that payload is binary compatible. plain/text should be encoded as a UTF8 byte array for example
    """
    def __init__(self, body: str, body_type: str = BrightsideMessageBodyType.text_plain) -> None:
        self._encoded_body = body.encode()
        self._body_type = body_type

    @property
    def value(self) -> str:
        """ Assumes that the body is text/plain i.e. json or xml and so returns the content as a string"""
        return self._encoded_body.decode()


@unique
class BrightsideMessageType(Enum):
    unacceptable = 1
    none = 2
    command = 3
    event = 4
    quit = 5


class BrightsideMessageHeader:
    """The header for our message. Note that this should agree with the Paramore.Brighter definition to ensure that
        different language implementations are compatible
    """
    def __init__(self, identity: UUID, topic: str, message_type: BrightsideMessageType, correlation_id: UUID = None,
                 reply_to: str = None, content_type: str = "text/plain", handled_count: int = None) -> None:
        self._id = identity
        self._topic = topic
        self._message_type = message_type
        self._correlation_id = correlation_id
        self._reply_to = reply_to
        self._content_type = content_type
        self._msg = None
        self._handled_count = handled_count if handled_count is not None else 0

    @property
    def handled_count(self) -> int:
        return self._handled_count

    @property
    def id (self) -> UUID:
        return self._id

    def increment_handled_count(self):
        self._handled_count += 1

    @property
    def topic(self) -> str:
        return self._topic

    @topic.setter
    def topic(self, value: str):
        self._topic = value

    @property
    def message_type(self) -> BrightsideMessageType:
        return self._message_type

    @property
    def correlation_id(self) -> UUID:
        return self._correlation_id

    @property
    def reply_to(self) -> str:
        return self._reply_to

    @reply_to.setter
    def reply_to(self, value: str):
        self._reply_to = value

    @property
    def content_type(self) -> str:
        return self._content_type

    @content_type.setter
    def content_type(self, value: str):
        self._content_type = value


class BrightsideMessage:
    """The representation of an on the wire message in Brighter. It abstracts the message typeof the underlying
    implementation and thus acts as a anti-corruption layer between us and an implementation specific message
    type
    """
    def __init__(self, message_header: BrightsideMessageHeader, message_body: BrightsideMessageBody) -> None:
        self._message_header = message_header
        self._message_body = message_body

    @property
    def header(self) -> BrightsideMessageHeader:
        return self._message_header

    @property
    def body(self) -> BrightsideMessageBody:
        return self._message_body

    def handled_count_reached(self, requeue_count: int) -> bool:
        return self._message_header.handled_count >= requeue_count

    @property
    def id(self) -> UUID:
        return self._message_header.id

    def increment_handled_count(self):
        self._message_header.increment_handled_count()


class BrightsideMessageStore(metaclass=ABCMeta):
    """ Brighter stores messages that it sends to a broker (before sending). This allows us to replay messages sent
    from a publisher to its subscribers. As a result, you can use non-durable queues (which are often more performant)
    if you are willing to trade 'at least once' delivery for 'retry on fail' and cope with duplicates.

    """
    @abstractmethod
    def add(self, message: BrightsideMessage):
        pass

    @abstractmethod
    def get_message(self, key: UUID):
        pass


class BrightsideProducer(metaclass=ABCMeta):
    """ The component that sends messages to a broker. Usually abstracts a socket connection to the broker, using
    a vendor specific client library.
    """
    @abstractmethod
    def send(self, message: BrightsideMessage):
        pass


class BrightsideConsumerConfiguration:
    """
    A class to encapsulate the configuration required to create a Brightside Consumer
    Required because we need to pass the parameters to a performer that runs the message pump on another thread
    """
    def __init__(self, pipeline: Queue, queue_name: str, routing_key: str, prefetch_count: int=1, is_durable: bool=False):
        self._pipeline = pipeline
        self._queue_name = queue_name
        self._routing_key = routing_key
        self._prefetch_count = prefetch_count
        self._is_durable = is_durable

    @property
    def pipeline(self):
        return self._pipeline

    @property
    def queue_name(self) -> str:
        return self._queue_name

    @property
    def routing_key(self) -> str:
        return self._routing_key

    @property
    def prefetch_count(self) -> int:
        return self._prefetch_count

    @property
    def is_durable(self) -> bool:
        return self._is_durable


class BrightsideConsumer(metaclass=ABCMeta):
    """The comoonent that receives messages from a broker. Usually abstracts a queue for subscribing to a topic on the
    broker i.e. a dynamic recepient list.
    """

    @abstractmethod
    def acknowledge(self, message: BrightsideMessage):
        pass

    @abstractmethod
    def cancel(self) -> None:
        pass

    @abstractmethod
    def has_acknowledged(self, message):
        pass

    @abstractmethod
    def purge(self):
        pass

    @abstractmethod
    def receive(self, timeout: int) -> BrightsideMessage:
        pass

    def requeue(self, message) -> None:
        pass

