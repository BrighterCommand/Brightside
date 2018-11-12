#!/usr/bin/env python
""""
File             : tests_arame_gateway.py
Author           : ian
Created          : 12-11-2018

Last Modified By : ian
Last Modified On : 12-11-2018
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
**********************************************************************i*s
"""

from multiprocessing import Queue
import unittest
from uuid import uuid4

from tests.config import TestConfig
from arame.gateway import ArameConsumer, ArameProducer
from brightside.connection import Connection
from brightside.messaging import BrightsideMessage, BrightsideConsumerConfiguration, BrightsideMessageBody, BrightsideMessageBodyType, BrightsideMessageHeader, BrightsideMessageType


class ArameGatewayHeartbeatTests(unittest.TestCase):

    test_topic = "kombu_gateway_tests"

    def setUp(self):
        config = TestConfig()
        self._connection = Connection(config.broker_uri, "paramore.brightside.exchange", is_durable=True)
        self._producer = ArameProducer(self._connection)
        self._pipeline = Queue()
        self._consumer = ArameConsumer(self._connection, BrightsideConsumerConfiguration(self._pipeline,
                                        "brightside_tests", self.test_topic, is_long_running_handler=True))

    def test_posting_a_message(self):
        """Given that I have an RMQ message producer
            when I send that message via the producer
            then I should be able to read that message via the consumer
        """
        header = BrightsideMessageHeader(uuid4(), self.test_topic, BrightsideMessageType.MT_COMMAND)
        body = BrightsideMessageBody("test content")
        message = BrightsideMessage(header, body)

        self._consumer.purge()

        self._producer.send(message)  # if errors with akes 1 positional argument but 2 were given then manually purge

        read_message = self._consumer.receive(3)
        self._consumer.acknowledge(read_message)

        self.assertEqual(message.id, read_message.id)
        self.assertEqual(message.body.value, read_message.body.value)
        self.assertTrue(self._consumer.has_acknowledged(read_message))
