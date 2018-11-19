"""
File             : sender.py
Author           : ian
Created          : 19-11-2018

Last Modified By : ian
Last Modified On : 19-11-2018
***********************************************************************
The MIT License (MIT)
Copyright  2018 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

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
import sys
import time

from multiprocessing import Queue

from arame.gateway import ArameConsumer
from brightside.connection import Connection
from brightside.command_processor import CommandProcessor, Request
from brightside.dispatch import ConsumerConfiguration, Dispatcher
from brightside.messaging import BrightsideConsumerConfiguration, BrightsideMessage
from brightside.registry import Registry
from arame.messaging import JsonRequestSerializer
from src.core import LongRunningCommand, LongRunningCommandHandler

KEYBOARD_INTERRUPT_SLEEP = 3    # How long before checking for a keyboard interrupt

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def command_processor_factory(channel_name: str):
    handler = LongRunningCommandHandler()

    subscriber_registry = Registry()
    subscriber_registry.register(LongRunningCommand, lambda: handler)

    command_processor = CommandProcessor(
        registry=subscriber_registry
    )
    return command_processor


def consumer_factory(connection: Connection, consumer_configuration: BrightsideConsumerConfiguration, logger: logging.Logger):
    return ArameConsumer(connection=connection, configuration=consumer_configuration, logger=logger)


def map_my_command_to_request(message: BrightsideMessage) -> Request:
    return JsonRequestSerializer(request=LongRunningCommand(), serialized_request=message.body.value).deserialize_from_json()


def run():
    pipeline = Queue()
    connection = Connection("amqp://guest:guest@localhost:5672//", "paramore.brightside.exchange", is_durable=True)
    configuration = BrightsideConsumerConfiguration(pipeline, "examples_long_running_queue", "long_running", is_long_running_handler=True)
    consumer = ConsumerConfiguration(connection, configuration, consumer_factory, command_processor_factory, map_my_command_to_request)
    dispatcher = Dispatcher({"HelloWorldCommand": consumer})

    dispatcher.receive()

    # poll for keyboard input to allow the user to quit monitoring
    while True:
        try:
            # just sleep unless we receive an interrupt i.e. CTRL+C
            time.sleep(KEYBOARD_INTERRUPT_SLEEP)
        except KeyboardInterrupt:
            dispatcher.end()
            sys.exit(1)


if __name__ == "__main__":
    run()
