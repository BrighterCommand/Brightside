"""
File             : dispatcher_tests.py
Author           : ian
Created          : 05-05-2017

Last Modified By : ian
Last Modified On : 05-05-2017
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
from multiprocessing import Event, Queue
import unittest
from uuid import uuid4
import time

from arame.messaging import JsonRequestSerializer
from core.connection import Connection
from core.messaging import BrightsideConsumerConfiguration, BrightsideMessageHeader, BrightsideMessageBody, \
    BrightsideMessage, BrightsideMessageType, BrightsideMessageBodyType
from serviceactivator.dispatch import ConsumerConfiguration, Dispatcher, DispatcherState, Performer
from tests.dispatcher_testdoubles import mock_command_processor_factory, mock_consumer_factory
from tests.handlers_testdoubles import MyCommand, MyEvent, map_my_command_to_request, map_my_event_to_request


class PerformerFixture(unittest.TestCase):
    def test_stop_performer(self):
        """
        Given that I have started a performer
        When I stop the performer
        Then it should terminate the pump
        :return:
        """
        request = MyCommand()
        pipeline = Queue()
        connection = Connection("amqp://guest:guest@localhost:5762/%2f", "brightside.perfomer.exchange")
        configuration = BrightsideConsumerConfiguration(pipeline, "performer.test.queue", "brightside.tests.mycommand")
        performer = Performer("test_channel", connection, configuration, mock_consumer_factory, mock_command_processor_factory, map_my_command_to_request)

        header = BrightsideMessageHeader(uuid4(), request.__class__.__name__, BrightsideMessageType.command)
        body = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json(),
                                     BrightsideMessageBodyType.application_json)
        message = BrightsideMessage(header, body)

        pipeline.put(message)

        started_event = Event()
        p = performer.run(started_event)

        started_event.wait()

        time.sleep(1)

        performer.stop()

        p.join()

        # We don't have much we can confirm other than: we reached here without error. The state is in a seperate process
        self.assertTrue(True)


class DispatcherFixture(unittest.TestCase):
    def test_stop_consumer(self):
        """Given that I have a dispatcher
            When I stop a consumer
            Then the performer should terminate
        """
        request = MyCommand()
        pipeline = Queue()
        connection = Connection("amqp://guest:guest@localhost:5762/%2f", "brightside.perfomer.exchange")
        configuration = BrightsideConsumerConfiguration(pipeline, "dispatcher.test.queue", "brightside.tests.mycommand")
        consumer = ConsumerConfiguration(connection, configuration, mock_consumer_factory, mock_command_processor_factory, map_my_command_to_request)
        dispatcher = Dispatcher({"MyCommand": consumer})

        header = BrightsideMessageHeader(uuid4(), request.__class__.__name__, BrightsideMessageType.command)
        body = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json(),
                                     BrightsideMessageBodyType.application_json)
        message = BrightsideMessage(header, body)

        pipeline.put(message)

        self.assertEqual(dispatcher.state, DispatcherState.ds_awaiting)

        dispatcher.receive()

        time.sleep(1)

        dispatcher.end()

        self.assertEqual(dispatcher.state, DispatcherState.ds_stopped)

    def test_restart_consumer(self):
        """Given that I have a dispatcher with all consumers stopped
            When I restart a consumer
            Then the dispatcher should have one running consumer
        """
        connection = Connection("amqp://guest:guest@localhost:5762/%2f", "brightside.perfomer.exchange")

        # First consumer
        request = MyCommand()
        pipeline_one = Queue()
        configuration_one = BrightsideConsumerConfiguration(pipeline_one, "restart_command.test.queue", "brightside.tests.mycommand")
        consumer_one = ConsumerConfiguration(connection, configuration_one, mock_consumer_factory, mock_command_processor_factory, map_my_command_to_request)

        header_one = BrightsideMessageHeader(uuid4(), request.__class__.__name__, BrightsideMessageType.command)
        body_one = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json(),
                                     BrightsideMessageBodyType.application_json)
        message_one = BrightsideMessage(header_one, body_one)

        pipeline_one.put(message_one)

        # Second consumer
        event = MyEvent()
        pipeline_two = Queue()
        configuration_two = BrightsideConsumerConfiguration(pipeline_two, "restart_event.test.queue", "brightside.tests.myevent")
        consumer_two = ConsumerConfiguration(connection, configuration_two, mock_consumer_factory, mock_command_processor_factory, map_my_event_to_request)

        header_two = BrightsideMessageHeader(uuid4(), event.__class__.__name__, BrightsideMessageType.event)
        body_two = BrightsideMessageBody(JsonRequestSerializer(request=event).serialize_to_json(),
                                         BrightsideMessageBodyType.application_json)
        message_two = BrightsideMessage(header_two, body_two)

        pipeline_two.put_nowait(message_two)

        # Dispatcher
        dispatcher = Dispatcher({"consumer_one": consumer_one, "consumer_two": consumer_two})

        # Consume the messages and stop
        self.assertEqual(dispatcher.state, DispatcherState.ds_awaiting)

        dispatcher.receive()

        time.sleep(1)

        dispatcher.end()

        self.assertEqual(dispatcher.state, DispatcherState.ds_stopped)

        #Now add a new message, restart a consumer, and eat
        event_three = MyEvent()
        header_three = BrightsideMessageHeader(uuid4(), event.__class__.__name__, BrightsideMessageType.event)
        body_three = BrightsideMessageBody(JsonRequestSerializer(request=event_three).serialize_to_json(),
                                         BrightsideMessageBodyType.application_json)
        message_three = BrightsideMessage(header_three, body_three)

        pipeline_two.put_nowait(message_three)

        dispatcher.open("consumer_two")

        time.sleep(1)

        dispatcher.end()

        self.assertEqual(dispatcher.state, DispatcherState.ds_stopped)






