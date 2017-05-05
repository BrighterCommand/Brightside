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
from multiprocessing import Array, Event, Process, Queue

from arame.gateway import ArameConnection, ArameConsumer
from core.channels import Channel
from core.message_factory import create_quit_message
from core.messaging import BrightsideConsumerConfiguration
from serviceactivator.message_pump import MessagePump


class Performer:
    def __init__(self, channel_name: str, connection: ArameConnection, consumer_configuration: BrightsideConsumerConfiguration) -> None:
        """
        Each Performer abstracts a process running a message pump.
        That process is forked from the parent, as we cannot guarantee a message pump is only I/O bound and thus will
        not scale because of the GIL.
        The Performer is how the supervisor (the dispatcher) tracks the workers it has created
        The Performer needs:
            A queue, whose purpose is to allow stop messages to be sent to the channel
            The properties needed to create a message pump or channel in the run method (which executes in the
            separate process) that can be communicatd to the child process via shared memory
        """
        self._pipeline = Queue()
        self._channel_name = channel_name
        self._connection = connection
        self._consumer_configuration = consumer_configuration

    def stop(self) -> None:
        self._pipeline.put(create_quit_message())

    def run(self) -> Process:
        event = Event()
        p = Process(target=_sub_process_main, args=(event, self._pipeline, self._channel_name, self._connection, self._consumer_configuration))
        p.start()

        event.wait(timeout=1)

        return p


def _sub_process_main(event: Event, pipeline: Queue, channel_name: str, connection: ArameConnection,
                     consumer_configuration: BrightsideConsumerConfiguration) -> None:
    """
    This is the main method for the sub=process, everything we need to create the message pump and
    channel it needs to be passed in as parameters that can be pickled as when we run they will be serialized
    into this process. The data should be value types, not reference types as we will receive a copy of the original.
    Inter-process communication is signalled by the event - to indicate startup - and the pipeline to facilitate a
    sentinel or stop message
    :param event: Used by the sub-process to signal that it is ready
    :param pipeline: Used to communicates 'stop' messages to the pump
    :param channel_name: The name we want to give the channel to the broker for identification
    :param connection: The 'broker' connection
    :param consumer_configuration: How to configure our consumer of messages from the channel
    :return:
    """

    #TODO: Create stdout logger for sub-process
    consumer = ArameConsumer(connection=connection, configuration=consumer_configuration)
    channel = Channel(name=channel_name, consumer=consumer, pipeline=pipeline)

    #TODO: Fix missing dependencies, including defaults that need passed in config values
    message_pump = MessagePump(command_processor=None, channel=channel, mapper_func=None,
                               timeout=500, unacceptable_message_limit=-1, requeue_count=3)

