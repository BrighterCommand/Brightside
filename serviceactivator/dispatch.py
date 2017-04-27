"""
File             : dispatch.py
Author           : ian
Created          : 04-21-2017

Last Modified By : ian
Last Modified On : 04-21-2017
***********************************************************************
The MIT License (MIT)
Copyright © 2017 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

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
from multiprocessing import Event, Process

from core.channels import Channel
from serviceactivator.message_pump import MessagePump


class Performer:
    def __init__(self, channel: Channel, message_pump: MessagePump):
        """
        We assume that the channel is the one the message pump operates on i.e. the queue of work
        We need a separate reference to it, so that we can signal termination

        TODO: We need to pass the channel's queue as a parameter to the pump as this is how we do
        inter-process communication with the multi-processing library.
        We need to switch to use multiprocessing.Queue
        """
        self._channel = channel
        self._message_pump =message_pump

    def stop(self) -> None:
        self._channel.stop()

    def run(self) -> Process:
        event = Event()
        p = Process(target=self._message_pump.run, args=(event,))
        p.start()

        event.wait(timeout=1)

        return p
