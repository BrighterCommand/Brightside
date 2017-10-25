#!/usr/bin/env python
"""
File             : tests_alchemy_store.py
Author           : ian
Created          : 09-28-2017

Last Modified By : ian
Last Modified On : 09-28-2017
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
************************************************************************
"""

import unittest
from uuid import uuid4

from alchemy_store.message_store import SqlAlchemyMessageStore
from brightside.messaging import BrightsideMessageHeader, BrightsideMessageBody, BrightsideMessageType, BrightsideMessage


class AlchemyStoreTests(unittest.TestCase):
    def test_get_from_message_store(self):
        """
            Given that I have a message in
            When I retrieve from the store by Id
            THen it should be found
        """
        store = SqlAlchemyMessageStore()

        message_id = uuid4()
        topic = "test topic"
        header = BrightsideMessageHeader(message_id, topic, BrightsideMessageType.MT_COMMAND)
        content = "test content"
        body = BrightsideMessageBody(content)
        message = BrightsideMessage(header, body)

        store.add(message)

        retreived_message = store.get_message(message_id)

        self.assertNotEqual(BrightsideMessageType.MT_NONE, retreived_message.header.message_type)
        self.assertEqual(message_id, retreived_message.id)
        self.assertEqual(content, retreived_message.body)








