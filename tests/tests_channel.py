"""
File             : channel_tests.py
Author           : ian
Created          : 02-15-2016

Last Modified By : ian
Last Modified On : 02-15-2016
***********************************************************************
The MIT License (MIT)
Copyright © 2015 Ian Cooper <ian_hammond_cooper@yahoo.co.uk>

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

from multiprocessing import Queue as Pipeline
import unittest
from uuid import uuid4

from brightside.messaging import BrightsideMessage, BrightsideMessageBody, BrightsideMessageHeader, BrightsideMessageType
from brightside.channels import Channel, ChannelState
from tests.channels_testdoubles import FakeConsumer


class ChannelFixture(unittest.TestCase):

    def test_handle_receive_on_a_channel(self):
        """
        Given that I have a channel
        When I receive on that channel
        Then I should get a message via the consumer
        """

        body = BrightsideMessageBody("test message")
        header = BrightsideMessageHeader(uuid4(), "test topic", BrightsideMessageType.MT_COMMAND)
        message = BrightsideMessage(header, body)

        fake_queue = [message]
        consumer = FakeConsumer(fake_queue)

        channel = Channel("test", consumer, Pipeline())

        msg = channel.receive(1)

        self.assertEqual(message.body.value, msg.body.value)
        self.assertEqual(message.header.topic, msg.header.topic)
        self.assertEqual(message.header.message_type, msg.header.message_type)
        self.assertEqual(0, len(fake_queue))  # We have read the queue
        self.assertTrue(channel.state == ChannelState.started)  # We don't stop because we consume a message

    def test_handle_stop(self):
        """
        Given that I have a channel
        When I receive a stop on that channel
        Then I should not process any further messages on that channel
        """

        body = BrightsideMessageBody("test message")
        header = BrightsideMessageHeader(uuid4(), "test topic", BrightsideMessageType.MT_COMMAND)
        message = BrightsideMessage(header, body)

        fake_queue = [message]
        consumer = FakeConsumer(fake_queue)

        channel = Channel("test", consumer, Pipeline())

        channel.stop()

        channel.receive(3)

        self.assertEqual(str(channel.name), "test")
        self.assertEqual(1, len(fake_queue))  # We have not read the queue
        self.assertTrue(channel.state == ChannelState.stopping)

    def test_handle_acknowledge(self):
        """
        Given that I have a channel
        When I acknowlege a message on that channel
        Then I should acknowledge the message on the consumer
        """

        body = BrightsideMessageBody("test message")
        header = BrightsideMessageHeader(uuid4(), "test topic", BrightsideMessageType.MT_COMMAND)
        message = BrightsideMessage(header, body)

        fake_queue = [message]
        consumer = FakeConsumer(fake_queue)

        channel = Channel("test", consumer, Pipeline())

        channel.acknowledge(message)

        self.assertTrue(consumer.has_acknowledged(message))

    def test_handle_requeue(self):
        """
        Given that I have a channel
        When I receive a requeue on that channel
        I should ask the the consumer to requeue the message
        """

        body = BrightsideMessageBody("test message")
        header = BrightsideMessageHeader(uuid4(), "test topic", BrightsideMessageType.MT_COMMAND)
        message = BrightsideMessage(header, body)

        fake_queue = []
        consumer = FakeConsumer(fake_queue)

        channel = Channel("test", consumer, Pipeline())

        channel.requeue(message)

        self.assertEqual(len(consumer), 1)


if __name__ == '__main__':
    unittest.main()








