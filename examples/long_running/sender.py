"""
File             : sender.py
Author           : ian
Created          : 19-11-2018

Last Modified By : ian
Last Modified On : 19-11-2018
***********************************************************************
The MIT License (MIT)
Copyright © 2018 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

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

from arame.gateway import ArameProducer
from arame.messaging import JsonRequestSerializer
from brightside.connection import Connection
from brightside.command_processor import CommandProcessor
from brightside.messaging import BrightsideMessageBody, BrightsideMessageHeader, BrightsideMessage, BrightsideMessageType
from brightside.registry import MessageMapperRegistry
from src.core import FakeMessageStore, LongRunningCommand

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def map_longrunningcommand_to_message(request: LongRunningCommand) -> BrightsideMessage:
    message_body = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json())
    message = BrightsideMessage(BrightsideMessageHeader(identity=request.id, topic="long_running", message_type=BrightsideMessageType.MT_COMMAND), message_body)
    return message


def run():
    message_store = FakeMessageStore()
    message_mapper_registry = MessageMapperRegistry()
    message_mapper_registry.register(LongRunningCommand, map_longrunningcommand_to_message)
    connection = Connection("amqp://guest:guest@localhost:5672//", "paramore.brightside.exchange", is_durable=True)
    producer = ArameProducer(connection)

    command_processor = CommandProcessor(
        message_mapper_registry=message_mapper_registry,
        message_store=message_store,
        producer=producer
    )

    job = LongRunningCommand(60)
    command_processor.post(job)


if __name__ == "__main__":
    run()
