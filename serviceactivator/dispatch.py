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
from multiprocessing import Event, Process, Queue
import logging
from typing import Callable

from arame.gateway import ArameConnection, ArameConsumer
from core.channels import Channel
from core.command_processor import CommandProcessor, Request
from core.message_factory import create_quit_message
from core.messaging import BrightsideConsumerConfiguration, BrightsideMessage
from serviceactivator.message_pump import MessagePump


class Performer:
    def __init__(self,
                 channel_name: str,
                 pipeline: Queue,
                 connection: ArameConnection,
                 consumer_configuration: BrightsideConsumerConfiguration,
                 command_processor_factory: Callable[[str], CommandProcessor],
                 mapper_func: Callable[[BrightsideMessage], Request],
                 logger: logging.Logger=None
                 ) -> None:
        """
        Each Performer abstracts a process running a message pump.
        That process is forked from the parent, as we cannot guarantee a message pump is only I/O bound and thus will
        not scale because of the GIL.
        The Performer is how the supervisor (the dispatcher) tracks the workers it has created
        The Performer needs:
        :param channel_name: The name of the channel we want to create a sub-process for
        :param connection: The connection to the broker
        :param We need a user supplied callback to create a commandprocessor with subscribers, policies, mappers etc.
        :param We need a user supplied callback to map on the wire messages to requests
        """
        # TODO: The paramater needs to be a connection, not an AramaConnection as we can't decide to create an Arame Consumer
        # here. Where do we make that choice?

        self._pipeline = pipeline
        self._channel_name = channel_name
        self._connection = connection
        self._consumer_configuration = consumer_configuration
        self._command_processor_factory = command_processor_factory
        self._mapper_func = mapper_func
        self._logger = logger or logging.getLogger(__name__)

    def stop(self) -> None:
        self._pipeline.put(create_quit_message())

    def run(self, started_event: Event) -> Process:

        p = Process(target=_sub_process_main, args=(
            started_event, self._pipeline, self._channel_name, self._connection, self._consumer_configuration,
            self._command_processor_factory,
            self._mapper_func))

        self._logger.debug("Starting worker process for channel: %s on exchange %s on server %s",
                           self._channel_name, self._connection.exchange, self._connection.amqp_uri)

        p.start()

        started_event.wait(timeout=1)

        return p


def _sub_process_main(started_event: Event,
                      pipeline: Queue,
                      channel_name: str,
                      connection: ArameConnection,
                      consumer_configuration: BrightsideConsumerConfiguration,
                      command_processor_factory: Callable[[str], CommandProcessor],
                      mapper_func: Callable[[BrightsideMessage], Request]) -> None:
    """
    This is the main method for the sub=process, everything we need to create the message pump and
    channel it needs to be passed in as parameters that can be pickled as when we run they will be serialized
    into this process. The data should be value types, not reference types as we will receive a copy of the original.
    Inter-process communication is signalled by the event - to indicate startup - and the pipeline to facilitate a
    sentinel or stop message
    :param started_event: Used by the sub-process to signal that it is ready
    :param pipeline: Used to communicates 'stop' messages to the pump
    :param channel_name: The name we want to give the channel to the broker for identification
    :param connection: The 'broker' connection
    :param consumer_configuration: How to configure our consumer of messages from the channel
    :param command_processor_factory: We need to register subscribers, policies, and task queues in this script
    :param mapper_func: We need to map between messages on the wire and our handlers
    :return:
    """

    logger = logging.getLogger(__name__)
    consumer = ArameConsumer(connection=connection, configuration=consumer_configuration, logger=logger)
    channel = Channel(name=channel_name, consumer=consumer, pipeline=pipeline)

    #TODO: Fix defaults that need passed in config values
    command_processor = command_processor_factory(channel_name)
    message_pump = MessagePump(command_processor=command_processor, channel=channel, mapper_func=mapper_func,
                               timeout=500, unacceptable_message_limit=None, requeue_count=None)

    logger.debug("Starting the message pump for %s", channel_name)
    message_pump.run(started_event)

