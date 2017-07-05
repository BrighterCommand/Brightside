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
import logging
import time
from enum import Enum
from multiprocessing import Event, Process
from threading import Thread
from typing import Callable, Dict

from brightside.channels import Channel
from brightside.command_processor import CommandProcessor, Request
from brightside.connection import Connection
from brightside.exceptions import ConfigurationException, MessagingException
from brightside.message_factory import create_quit_message
from brightside.message_pump import MessagePump
from brightside.messaging import BrightsideConsumerConfiguration, BrightsideConsumer, BrightsideMessage


class Performer:
    def __init__(self,
                 channel_name: str,
                 connection: Connection,
                 consumer_configuration: BrightsideConsumerConfiguration,
                 consumer_factory: Callable[[Connection, BrightsideConsumerConfiguration, logging.Logger], BrightsideConsumer],
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
        :param consumer_factory: We need a user supplied callback to provide us an instance of the concumer for
            the broker we are using. Arame? Something else?
        :param command_processor_factory: We need a user supplied callback to create a commandprocessor with
            subscribers, policies, outgoing tasks queues etc.
        :param mapper_func: We need a user supplied callback to map on the wire messages to requests
        """
        # TODO: The paramater needs to be a connection, not an AramaConnection as we can't decide to create an Arame Consumer
        # here. Where do we make that choice?

        self._channel_name = channel_name
        self._connection = connection
        self._consumer_configuration = consumer_configuration
        self._consumer_factory = consumer_factory
        self._command_processor_factory = command_processor_factory
        self._mapper_func = mapper_func
        self._logger = logger or logging.getLogger(__name__)

    def stop(self) -> None:
        self._consumer_configuration.pipeline.put(create_quit_message())

    def run(self, started_event: Event) -> Process:

        p = Process(target=_sub_process_main, args=(
            started_event,
            self._channel_name,
            self._connection,
            self._consumer_configuration,
            self._consumer_factory,
            self._command_processor_factory,
            self._mapper_func))

        self._logger.debug("Starting worker process for channel: %s on exchange %s on server %s",
                           self._channel_name, self._connection.exchange, self._connection.amqp_uri)

        p.start()

        started_event.wait(timeout=1)

        return p


def _sub_process_main(started_event: Event,
                      channel_name: str,
                      connection: Connection,
                      consumer_configuration: BrightsideConsumerConfiguration,
                      consumer_factory: Callable[[Connection, BrightsideConsumerConfiguration, logging.Logger], BrightsideConsumer],
                      command_processor_factory: Callable[[str], CommandProcessor],
                      mapper_func: Callable[[BrightsideMessage], Request]) -> None:
    """
    This is the main method for the sub=process, everything we need to create the message pump and
    channel it needs to be passed in as parameters that can be pickled as when we run they will be serialized
    into this process. The data should be value types, not reference types as we will receive a copy of the original.
    Inter-process communication is signalled by the event - to indicate startup - and the pipeline to facilitate a
    sentinel or stop message
    :param started_event: Used by the sub-process to signal that it is ready
    :param channel_name: The name we want to give the channel to the broker for identification
    :param connection: The 'broker' connection
    :param consumer_configuration: How to configure our consumer of messages from the channel
    :param consumer_factory: Callback to create the consumer. User code as we don't know what consumer library they
        want to use. Arame? Something else?
    :param command_processor_factory: Callback to  register subscribers, policies, and task queues then build command
        processor. User code that provides us with their requests and handlers
    :param mapper_func: We need to map between messages on the wire and our handlers
    :return:
    """

    logger = logging.getLogger(__name__)
    consumer = consumer_factory(connection, consumer_configuration, logger)
    channel = Channel(name=channel_name, consumer=consumer, pipeline=consumer_configuration.pipeline)

    #TODO: Fix defaults that need passed in config values
    command_processor = command_processor_factory(channel_name)
    message_pump = MessagePump(command_processor=command_processor, channel=channel, mapper_func=mapper_func,
                               timeout=500, unacceptable_message_limit=None, requeue_count=None)

    logger.debug("Starting the message pump for %s", channel_name)
    message_pump.run(started_event)


class ConsumerConfiguration:
    def __init__(self,
                 connection: Connection,
                 consumer: BrightsideConsumerConfiguration,
                 consumer_factory: Callable[[Connection, BrightsideConsumerConfiguration, logging.Logger], BrightsideConsumer],
                 command_processor_factory: Callable[[str], CommandProcessor],
                 mapper_func: Callable[[BrightsideMessage], Request]) -> None:
        """
        The configuration parameters for one consumer - can create one or more performers from this, each of which is
        a message pump reading froma queue
        :param connection: The connection to the broker
        :param consumer: The consumer we want to create (routing key, queue etc)
        :param consumer_factory: A factory to create a consumer to read from a broker, a given implementation i.e. arame
        :param the command processor factory creates a command procesoor configured for a pipeline
        :param mapper_func: Maps between messages on the queue and requests (commnands/events)
        """
        self._connection = connection
        self._consumer = consumer
        self._consumer_factory = consumer_factory
        self._command_processor_factory = command_processor_factory
        self._mapper_func = mapper_func

    @property
    def connection(self) -> Connection:
        return self._connection

    @property
    def brightside_configuration(self) -> BrightsideConsumerConfiguration:
        return self._consumer

    @property
    def consumer_factory(self) -> Callable[[Connection, BrightsideConsumerConfiguration, logging.Logger], BrightsideConsumer]:
        return self._consumer_factory

    @property
    def command_processor_factory(self):
        return self._command_processor_factory

    @property
    def mapper_func(self) -> Callable[[BrightsideMessage], Request]:
        return self._mapper_func


class DispatcherState(Enum):
    ds_awaiting = 0,
    ds_notready = 1,
    ds_running = 2,
    ds_stopped = 3,
    ds_stopping = 4


class Dispatcher:
    """
    The dispatcher orchestrates the creation of consumers, where a consumer is the sub-process that runs a message pump
    to consumer messages from a given channel and dispatch to handlers. The dispatcher can start more than one performer
    for a given channel.
    The dispatcher also orchestrates the shutdown of consumers. It does this by posting a stop message into each running
    consumers queue, thus allowing the current handler to run to completion but killing the consumer before it can
    consume another work item from the queue.
    As such the dispatcher tracks consumer instances.
    In addition, as we must pass a factory method to the sub-process that creates the command processor for that channel
    i.e. handler and policy registration, outgoing queues, the Dispatcher also acts a registry of those factory methods
    for individual channels.
    THe dispatcher uses a thread to 'stay running' until end is called. This means that receive is non-blocking. The
    supervisor thread yields regularly to avoid spinning the CPU. This means there can be a delay between signalling to
    end and the shutdown beginning.
    Shutdown will finish work in progress, as it inserts a quit message in the queue that gets consumerd 'next'
    """
    def __init__(self, consumers: Dict[str, ConsumerConfiguration]) -> None:
        self._state = DispatcherState.ds_notready

        self._consumers = consumers

        self._performers = {k: Performer(
                            k,
                            v.connection,
                            v.brightside_configuration,
                            v.consumer_factory,
                            v.command_processor_factory,
                            v.mapper_func)
                            for k, v in self._consumers.items()}

        self._running_performers = {}
        self._supervisor = None

        self._state = DispatcherState.ds_awaiting

    @property
    def state(self):
        return self._state

    def receive(self):

        def _receive(dispatcher: Dispatcher, initialized: Event) -> None:
            for k, v in self._performers.items():
                event = Event()
                dispatcher._running_performers[k] = v.run(event)
                event.wait(3) # TODO: Do we want to configure this polling interval?

            initialized.set()

            while self._state == DispatcherState.ds_running:
                time.sleep(5)  # yield to avoid spinning, between checking for changes to state

        if self._state == DispatcherState.ds_awaiting:
            initialized = Event()
            self._supervisor = Thread(target=_receive, args=(self, initialized))
            initialized.wait(5)  # TODO: Should this be number of performs and configured with related?
            self._state = DispatcherState.ds_running
            self._supervisor.start()

    def end(self):
        if self._state == DispatcherState.ds_running:
            for channel, process in self._running_performers.items():
                self._performers[channel].stop()
                process.join(10)  # TODO: We really want to make this configurable

            self._state == DispatcherState.ds_stopping
            self._supervisor.join(5)
            self._running_performers.clear()
            self._supervisor = None

        self._state = DispatcherState.ds_stopped

        # Do we want to determine if any processes have failed to complete Within the time frame

    def open(self, consumer_name: str) -> None:
        # TODO: Build then refactor with receive
        # Find the consumer
        if consumer_name not in self._consumers:
            raise ConfigurationException("The consumer {} could not be found, did you register it?".format(consumer_name))

        consumer = self._consumers[consumer_name]
        performer = Performer(consumer_name,
                              consumer.connection,
                              consumer.brightside_configuration,
                              consumer.consumer_factory,
                              consumer.command_processor_factory,
                              consumer.mapper_func)
        self._performers[consumer_name] = performer

        # if we have a supervisor thread
        if self._state == DispatcherState.ds_running:
        # start and add to items monitored by supervisor (running performers)
            pass
        # else
        elif self._state == DispatcherState.ds_stopped:
        # start the supervisor with the single consumer
            self._state = DispatcherState.ds_awaiting
            self.receive()
        else:
            raise MessagingException("Dispatcher in a un-recognised state to open new connection; state was {}", self._state)




