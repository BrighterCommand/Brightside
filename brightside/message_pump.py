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

from contextlib import contextmanager
import logging
import time
from typing import Callable
from threading import current_thread, Event

from brightside.command_processor import CommandProcessor, Request
from brightside.channels import Channel
from brightside.exceptions import ChannelFailureException, ConfigurationException, DeferMessageException
from brightside.messaging import BrightsideMessage, BrightsideMessageHeader, BrightsideMessageType


@contextmanager
def heartbeat(channel: Channel):
    channel.start_heartbeat()
    yield
    channel.end_heartbeat()


class MessagePump:
    def __init__(self, command_processor: CommandProcessor,
                 channel: Channel,
                 mapper_func: Callable[[BrightsideMessage], Request],
                 timeout: int = None,
                 unacceptable_message_limit: int = None,
                 requeue_count: int = None) -> None:
        self._command_processor = command_processor
        self._channel = channel
        self._mapper_func = mapper_func
        self._timeout = timeout / 1000 if timeout else 0.5
        self._logger = logging.getLogger(__name__)
        self._unacceptable_message_limit = unacceptable_message_limit if unacceptable_message_limit else 500
        self._unacceptable_message_count = 0
        self._requeue_count = requeue_count

    def run(self, started_event: Event = None) -> None:

        # Can be used to signal that we have started to caller
        if started_event is not None:
            started_event.set()

        while True:

            if self._unacceptable_message_limit_reached():
                self._channel.end()
                break

            message = None
            try:
                self._logger.debug("MessagePump: Receiving messages from {} on thread # {}".format(
                    self._channel.name, current_thread().name))

                message = self._channel.receive(self._timeout)
            except ChannelFailureException:
                self._logger.warning("MessagePump: ChannelFailureException receiving messages from {} on thread # {}".format(
                    self._channel.name, current_thread().name), exc_info=1)
                continue
            except Exception:
                self._logger.warning("MessagePump: Exception receiving messages from {} on thread # {}".format(
                    self._channel.name, current_thread().name), exc_info=1)

            if message is None:
                raise ChannelFailureException("Could not receive message. Note that should return BrightsideMessageType.none from an empty queeu")
            elif message.header.message_type == BrightsideMessageType.MT_NONE:
                time.sleep(self._timeout)
                continue
            elif message.header.message_type == BrightsideMessageType.MT_QUIT:
                self._logger.debug("MessagePump: Quit receiving messages from {} on thread # {}".format(
                    self._channel.name, current_thread().name))
                self._channel.end()
                break
            elif message.header.message_type == BrightsideMessageType.MT_UNACCEPTABLE:
                self._logger.debug("MessagePump: Failed to parse a message from the incoming message with id {} from {} on thread # {} ".format(
                    message.id, self._channel.name, current_thread().name))
                self._acknowledge_message(message)
                self._increment_unacceptable_message_count()
                continue

            with (heartbeat(self._channel)):
                try:
                    # Serviceable message
                    request = self._translate_message(message)
                    self._dispatch_message(message.header, request)

                except DeferMessageException:
                    self._requeue_message(message)
                    continue
                except ConfigurationException:
                    raise
                except Exception as ex:
                    self._logger.error("MessagePump: Failed to dispatch the message with id {} from {} on thread # {} due to {}".format(
                        message.id, self._channel.name, current_thread().name, ex))

                self._acknowledge_message(message)

        self._logger.debug("MessagePump: Finished running message loop, no longer receiving messages from {} on thread # {}".format(
            self._channel.name, current_thread().name))

        self._channel.end()

    def _acknowledge_message(self, message: BrightsideMessage) -> None:
        self._logger.debug("MessagePump: Acknowledge message {} from {} on thread # {}".format(
            message.id, self._channel.name, current_thread().name))
        self._channel.acknowledge(message)

    def _discard_requeued_messages_enabled(self):
        return self._requeue_count is not None

    def _dispatch_message(self, message_header: BrightsideMessageHeader, request: Request) -> None:
        if message_header.message_type == BrightsideMessageType.MT_COMMAND:
            self._command_processor.send(request)
        elif message_header.message_type == BrightsideMessageType.MT_EVENT:
            self._command_processor.publish(request)

    def _increment_unacceptable_message_count(self) -> int:
        self._unacceptable_message_count += 1
        return self._unacceptable_message_count

    def _requeue_message(self, message: BrightsideMessage) -> None:
        message.increment_handled_count()

        if self._discard_requeued_messages_enabled():
            if message.handled_count_reached(self._requeue_count):
                self._logger.error("MessagePump: Have tried {} times to handle this message {} dropping message \n. Message Body {} ".format(
                    self._requeue_count, message.id, message.body.value))
                self._channel.acknowledge(message)
                return

        self._logger.debug("MessagePump: Re-queueing message {} from {}".format(
                message.id, self._channel.name))
        self._channel.requeue(message)

    def _translate_message(self, message: BrightsideMessage)-> Request:
        if self._mapper_func is None:
            raise ConfigurationException("Missing Mapper Function for message topic {}".format(message.header.topic))
        return self._mapper_func(message)

    def _unacceptable_message_limit_reached(self) -> bool:
        return self._unacceptable_message_count >= self._unacceptable_message_limit







