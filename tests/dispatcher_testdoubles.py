"""
File             : dispatcher_testdoubles.py
Author           : ian
Created          : 06-05-2017

Last Modified By : ian
Last Modified On : 06-05-2017
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
from unittest.mock import Mock
import logging

from arame.gateway import ArameConsumer
from brightside.command_processor import CommandProcessor
from brightside.connection import Connection
from brightside.messaging import BrightsideConsumer, BrightsideConsumerConfiguration
from brightside.message_factory import create_null_message


def mock_command_processor_factory(channel_name: str):
    """
    callback function to create a command processor
    channel_name is intended to help and implementor set up the command processor, but its not needed here
    """
    mock_command_processor = Mock(spec=CommandProcessor)
    return mock_command_processor


def mock_consumer_factory(connection: Connection, consumer_configuration: BrightsideConsumerConfiguration,
                          logger: logging.Logger):
    consumer = Mock(spec=BrightsideConsumer)
    null_message = create_null_message()
    consumer_spec = {"receive.return_value": null_message}
    consumer.configure_mock(**consumer_spec)
    return consumer


def arame_consuemr_factory(connection: Connection, consumer_configuration: BrightsideConsumerConfiguration,
                          logger: logging.Logger) -> BrightsideConsumer:
    return ArameConsumer(connection=connection, configuration=consumer_configuration, logger=logger)
