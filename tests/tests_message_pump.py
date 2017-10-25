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
import unittest
from threading import Event, Thread
from unittest.mock import Mock
from uuid import uuid4

from arame.messaging import JsonRequestSerializer
from brightside.channels import Channel
from brightside.command_processor import CommandProcessor
from brightside.exceptions import ConfigurationException, DeferMessageException
from brightside.message_factory import create_quit_message
from brightside.message_pump import MessagePump
from brightside.messaging import BrightsideMessage, BrightsideMessageBody, BrightsideMessageBodyType, BrightsideMessageHeader, BrightsideMessageType
from tests.handlers_testdoubles import MyCommandHandler, MyCommand, map_my_command_to_request
from tests.message_pump_doubles import FakeChannel


class MessagePumpFixture(unittest.TestCase):

    def test_the_pump_should_dispatch_a_command_processor(self):
        """
            Given that I have a message pump for a channel
             When I read a message from that channel
             Then the message should be dispatched to a handler
        """
        handler = MyCommandHandler()
        request = MyCommand()
        channel = Mock(spec=Channel)
        command_processor = Mock(spec=CommandProcessor)

        message_pump = MessagePump(command_processor, channel, map_my_command_to_request)

        header = BrightsideMessageHeader(uuid4(), request.__class__.__name__, BrightsideMessageType.MT_COMMAND)
        body = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json(),
                                     BrightsideMessageBodyType.application_json)
        message = BrightsideMessage(header, body)

        quit_message = create_quit_message()

        # add messages to that when channel is called it returns first message then quit message
        response_queue = [message, quit_message]
        channel_spec = {"receive.side_effect": response_queue}
        channel.configure_mock(**channel_spec)

        message_pump.run()

        channel.receive.assert_called_with(0.5)
        self.assertEqual(channel.receive.call_count, 2)
        self.assertTrue(command_processor.send.call_count, 1)
        self.assertEqual(channel.acknowledge.call_count, 1)


    # TODO: Test for message pump is missing
    def test_the_pump_should_fail_on_a_missing_message_mapper(self):
        """
            Given that I have a message pump for a channel
             When there is no message mapper for that channel
             Then we shhould throw an exception to indicate a configuration error
        """
        handler = MyCommandHandler()
        request = MyCommand()
        channel = Mock(spec=Channel)
        command_processor = Mock(spec=CommandProcessor)

        message_pump = MessagePump(command_processor, channel, None)

        header = BrightsideMessageHeader(uuid4(), request.__class__.__name__, BrightsideMessageType.MT_COMMAND)
        body = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json(),
                                     BrightsideMessageBodyType.application_json)
        message = BrightsideMessage(header, body)

        quit_message = create_quit_message()

        # add messages to that when channel is called it returns first message then qui tmessage
        response_queue = [message, quit_message]
        channel_spec = {"receive.side_effect": response_queue}
        channel.configure_mock(**channel_spec)

        excepton_caught = False
        try:
            message_pump.run()
        except ConfigurationException:
            excepton_caught = True

        self.assertTrue(excepton_caught)


    def test_the_pump_should_acknowledge_and_discard_an_unacceptable_message(self):
        """
            Given that I have a message pump for a channel
             When I cannot read the message received from that channel
             Then I should acknowledge the message to dicard it
        """
        handler = MyCommandHandler()
        request = MyCommand()
        channel = Mock(spec=Channel)
        command_processor = Mock(spec=CommandProcessor)

        message_pump = MessagePump(command_processor, channel, map_my_command_to_request)

        header = BrightsideMessageHeader(uuid4(), request.__class__.__name__, BrightsideMessageType.MT_UNACCEPTABLE)
        body = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json(),
                                     BrightsideMessageBodyType.application_json)
        message = BrightsideMessage(header, body)

        quit_message = create_quit_message()

        # add messages to that when channel is called it returns first message then qui tmessage
        response_queue = [message, quit_message]
        channel_spec = {"receive.side_effect": response_queue}
        channel.configure_mock(**channel_spec)

        message_pump.run()

        channel.receive.assert_called_with(0.5)
        self.assertEqual(channel.receive.call_count, 2)
        # We acknowledge so that a 'poison pill' message cannot block our queue
        self.assertEqual(channel.acknowledge.call_count, 1)
        # Does not send the message, just discards it
        self.assertEqual(command_processor.send.call_count, 0)

    def test_the_pump_should_limit_unacceptable_messages(self):
        """
            Given that I have a message pump for a channel
             When I cannot read the message received from that channel
             Then I should acknowledge the message to dicard it
        """
        handler = MyCommandHandler()
        request = MyCommand()
        channel = Mock(spec=Channel)
        command_processor = Mock(spec=CommandProcessor)
        unacceptable_message_limit = 3

        message_pump = MessagePump(command_processor, channel, map_my_command_to_request, unacceptable_message_limit=unacceptable_message_limit)

        header = BrightsideMessageHeader(uuid4(), request.__class__.__name__, BrightsideMessageType.MT_UNACCEPTABLE)
        body = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json(),
                                     BrightsideMessageBodyType.application_json)
        message_one = BrightsideMessage(header, body)
        message_two = BrightsideMessage(header, body)
        message_three = BrightsideMessage(header, body)
        message_four = BrightsideMessage(header, body)

        quit_message = create_quit_message()

        # add messages to that when channel is called it returns first message then qui tmessage
        response_queue = [message_one, message_two, message_three, message_four, quit_message]
        channel_spec = {"receive.side_effect": response_queue}
        channel.configure_mock(**channel_spec)

        message_pump.run()

        # Should acknowledge first three so that a 'poison pill' message cannot block our queue
        self.assertEqual(channel.acknowledge.call_count, unacceptable_message_limit)
        # We should dispose of the channel, by sending ourselves a quit messages
        self.assertEqual(channel.end.call_count, 1)
        # Does not send the message, just discards it
        self.assertEqual(command_processor.send.call_count, 0)

    def test_the_pump_should_reque_deferred_messages(self):
        """
            Given that I have a message pump for a channel
             When the handler raises a defer message exception for that message
             Then requeue the message
        """
        handler = MyCommandHandler()
        request = MyCommand()
        channel = Mock(spec=Channel)
        command_processor = Mock(spec=CommandProcessor)

        message_pump = MessagePump(command_processor, channel, map_my_command_to_request)

        header = BrightsideMessageHeader(uuid4(), request.__class__.__name__, BrightsideMessageType.MT_COMMAND)
        body = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json(),
                                     BrightsideMessageBodyType.application_json)
        message = BrightsideMessage(header, body)

        quit_message = create_quit_message()

        # add messages to that when channel is called it returns first message then qui tmessage
        response_queue = [message, quit_message]
        channel_spec = {"receive.side_effect": response_queue}
        channel.configure_mock(**channel_spec)

        requeue_spec = {"send.side_effect": DeferMessageException()}
        command_processor.configure_mock(**requeue_spec)

        message_pump.run()

        channel.receive.assert_called_with(0.5)
        self.assertEqual(channel.receive.call_count, 2)
        self.assertTrue(command_processor.send.call_count, 1)
        self.assertEqual(channel.requeue.call_count, 1)

    def test_handle_requeue_has_upper_bound(self):
        """
        Given that I have a channel
        When I receive a requeue on that channel
        I should ask the consumer to requeue, up to a retry limit
        So that poison messages do not fill our queues
        """
        handler = MyCommandHandler()
        request = MyCommand()
        channel = FakeChannel(name="MyCommand")
        command_processor = Mock(spec=CommandProcessor)

        message_pump = MessagePump(command_processor, channel, map_my_command_to_request, requeue_count=3)

        header = BrightsideMessageHeader(uuid4(), request.__class__.__name__, BrightsideMessageType.MT_COMMAND)
        body = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json(),
                                     BrightsideMessageBodyType.application_json)
        message = BrightsideMessage(header, body)

        channel.add(message)

        requeue_spec = {"send.side_effect": DeferMessageException()}

        command_processor.configure_mock(**requeue_spec)

        started_event = Event()

        t = Thread(target=message_pump.run, args=(started_event,))

        t.start()

        started_event.wait()

        time.sleep(1)

        channel.stop()

        t.join()

        self.assertTrue(command_processor.send.call_count, 3)

if __name__ == '__main__':
    unittest.main()


