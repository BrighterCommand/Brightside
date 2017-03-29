""""
File             : message_pump.py
Author           : ian
Created          : 11-23-2016

Last Modified By : ian
Last Modified On : 11-23-2016
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

import time
from typing import Callable

from core.command_processor import CommandProcessor, Request
from core.channels import Channel
from core.exceptions import ChannelFailureException, ConfigurationException
from core.messaging import BrightsideMessage, BrightsideMessageHeader, BrightsideMessageType


class MessagePump:
    def __init__(self, command_processor: CommandProcessor,
                 channel: Channel,
                 mapper_func: Callable[[BrightsideMessage], Request],
                 timeout: int = None) -> None:
        self._command_processor = command_processor
        self._channel = channel
        self._mapper_func = mapper_func
        self._timeout = 1000 / timeout if timeout else 0.5

    def run(self) -> None:
        while True:
            _message = None
            try:
                _message = self._channel.receive(self._timeout)
            except ChannelFailureException:
                break

            if _message is None:
                time.sleep(self._timeout)
                continue
            elif _message.header.message_type == BrightsideMessageType.quit:
                # TODO: Log intent to break
                break
            elif _message.header.message_type == BrightsideMessageType.unacceptable:
                # TODO: Log an unacceptable message
                self._acknowledge_message(_message)
                continue

            # Serviceable message
            request = self._translate_message(_message)
            self._dispatch_message(_message._message_header, request)

            self._acknowledge_message(_message)

    def _translate_message(self, message: BrightsideMessage)-> Request:
        if self._mapper_func is None:
            raise ConfigurationException("Missing Mapper Function for message topic {}".format(message.header.topic))
        return self._mapper_func(message)

    def _dispatch_message(self, message_header: BrightsideMessageHeader, request: Request) -> None:
        if message_header.message_type == BrightsideMessageType.command:
            self._command_processor.send(request)
        elif message_header.message_type == BrightsideMessageType.event:
            self._command_processor.publish(request)

    def _acknowledge_message(self, message: BrightsideMessage) -> None:
        # TODO: We need to log acknowledging a message
        self._channel.acknowledge(message)





