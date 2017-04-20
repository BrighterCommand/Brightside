""""
File             : message_pump_doubles.py
Author           : ian
Created          : 12-09-2016

Last Modified By : ian
Last Modified On : 12-09-2016
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

from queue import Queue

from core.channels import ChannelFailureException, ChannelName, ChannelState
from core.messaging import BrightsideMessage
from core.message_factory import create_null_message, create_quit_message


class FakeChannel:
    def __init__(self, name: str) -> None:
        self._name = ChannelName(name)
        self._queue = Queue()
        self._state = ChannelState.initialized

    def __len__(self):
        return self._queue.qsize()

    def acknowledge(self, message: BrightsideMessage):
        pass

    def add(self, message: BrightsideMessage):
        self._queue.put(message)

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
            return create_null_message()

        return self._queue.get(block=True, timeout=timeout)

    @property
    def state(self) -> ChannelState:
        return self._state

    def stop(self):
        self._queue.put(create_quit_message())
        self._state = ChannelState.stopping

    def requeue(self, message):
        self._queue.put(message)
