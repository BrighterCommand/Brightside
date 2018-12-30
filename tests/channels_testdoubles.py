"""
File             : handlers_testdoubles.py
Author           : ian
Created          : 11-21-2016

Last Modified By : ian
Last Modified On : 11-24-2018
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

import threading
from typing import List

from brightside.messaging import BrightsideConsumer


class FakeConsumer(BrightsideConsumer):
    """The fake consumer is a test double for a consumer wrapping messaging middleware.
        To use it, just add BrighsideMessage(s) to the queue and the call receive to pop
        then off the stack. Purge, will clean the queue
    """

    def __init__(self, queue: List):
        self._acknowledged_message = None
        self._queue = queue

    def __len__(self):
        return len(self._queue)

    def acknowledge(self, message):
        self._acknowledged_message = message

    def cancel(self) -> None:
        pass

    def has_acknowledged(self, message):
        return (self._acknowledged_message is not None) and (self._acknowledged_message.id == message.id)

    def purge(self):
        self._queue.clear()

    def receive(self, timeout: int):
        return self._queue.pop()

    def requeue(self, message):
        self._queue.append(message)

    def run_heartbeat_continuously(self) -> threading.Event:
        return threading.Event()

    def stop(self):
        pass

