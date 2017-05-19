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
from tests.dispatcher_testdoubles import mock_command_processor_factory
from tests.handlers_testdoubles import MyCommand, map_to_request


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
        performer = Performer("test_channel", connection, configuration, mock_command_processor_factory, map_to_request)

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
        configuration = BrightsideConsumerConfiguration(pipeline, "performer.test.queue", "brightside.tests.mycommand")
        consumer = ConsumerConfiguration(connection, configuration, mock_command_processor_factory, map_to_request)
        dispatcher = Dispatcher({"MyCommand": consumer})

        header = BrightsideMessageHeader(uuid4(), request.__class__.__name__, BrightsideMessageType.command)
        body = BrightsideMessageBody(JsonRequestSerializer(request=request).serialize_to_json(),
                                     BrightsideMessageBodyType.application_json)
        message = BrightsideMessage(header, body)

        pipeline.put(message)

        self.assertEqual(dispatcher.state, DispatcherState.ds_awaiting)

        d = dispatcher.receive()

        time.sleep(1)

        dispatcher.end()

        d.join()

        self.assertEqual(dispatcher.state, DispatcherState.ds_stopped)




