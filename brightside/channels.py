"""
File             : channels.py
Author           : ian
Created          : 11-21-2016

Last Modified By : ian
Last Modified On : 11-21-2016
***********************************************************************
The MIT License (MIT)
Copyright © 2015 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

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
from enum import Enum
from multiprocessing import Queue

from brightside.exceptions import ChannelFailureException
from brightside.messaging import BrightsideConsumer, BrightsideMessage
from brightside.message_factory import create_quit_message


class ChannelState(Enum):
    initialized = 0
    started = 1
    stopping = 2
    stopped = 3


class ChannelName:
    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def value(self) -> str:
        return self._name

    def __str__(self) -> str:
        return self._name


class Channel:
    """
    A queue for one data type
    Uses BrightsideConsumer to be independent of the underlying implementation of the consumer i.e. rmq, redis, etc.
    It uses a buffer over the backing service queue, this allows us to insert control messages into the channel.
    We use control message to stop the consumption of messages from the channel
    """
    def __init__(self, name: str, consumer: BrightsideConsumer, pipeline: Queue) -> None:
        self._consumer = consumer
        self._name = ChannelName(name)
        self._queue = pipeline
        self._state = ChannelState.initialized

    def __len__(self):
        return self._queue.qsize()

    def acknowledge(self, message: BrightsideMessage):
        self._consumer.acknowledge(message)

    def end(self) -> None:
        self._state = ChannelState.stopped

    @property
    def name(self) -> ChannelName:
        return self._name

    def receive(self, timeout: int) -> BrightsideMessage:
        if self._state is ChannelState.stopped:
            raise ChannelFailureException("Channcle has been stopped, cannot resume listening")

        if self._state is ChannelState.initialized:
            self._state = ChannelState.started

        if not self._queue.empty():
            return self._queue.get(block=True, timeout=timeout)

        return self._consumer.receive(timeout=timeout)

    @property
    def state(self) -> ChannelState:
        return self._state

    def stop(self):
        self._queue.put(create_quit_message())
        self._state = ChannelState.stopping

    def requeue(self, message):
        self._consumer.requeue(message)



