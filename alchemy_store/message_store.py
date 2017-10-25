""""
File             : message_store.py
Author           : ian
Created          : 09-01-2016

Last Modified By : ian
Last Modified On : 10-02-2017
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
**********************************************************************i*
"""

from datetime import datetime
import json
from uuid import UUID, uuid4


from alchemy_store import engine, messages
from brightside.messaging import BrightsideMessage, BrightsideMessageHeader, BrightsideMessageBody, BrightsideMessageType, BrightsideMessageStore
from sqlalchemy import select


def  deserialize_header_bag(bag: str) -> dict:
    if bag is not None:
        return json.loads(bag)
    else:
        return dict()


# def serialize_header_bag(json: Dict[]) ->

def create_empty_message() -> BrightsideMessage:
    return BrightsideMessage(
        BrightsideMessageHeader(identity=uuid4(), topic="", message_type=BrightsideMessageType.MT_NONE),
        BrightsideMessageBody(""))


def create_message(row) -> BrightsideMessage:
    bag = deserialize_header_bag(row[messages.c.HeaderBag])
    message = BrightsideMessage(
        BrightsideMessageHeader(
            identity=row[messages.c.MessageId],
            topic=row[messages.c.Topic],
            message_type=row[messages.c.MessageType],
            header_bag=bag),
        BrightsideMessageBody(row[messages.c.Body])
    )
    return message


class SqlAlchemyMessageStore(BrightsideMessageStore):
    def __init__(self):
        super().__init__()

    def add(self, message: BrightsideMessage) -> None:
        ins = messages.insert().values(
            MessageId=message.id,
            Topic=message.header.topic,
            MessageType=message.header.message_type,
            Timestamp=datetime.utcnow(),
            Body=message.body.value
            )
        conn = engine.connect()
        with conn.begin() as trans:
            conn.execute(ins)
            trans.commit()
        conn.close()

    def get_message(self, key: UUID) -> BrightsideMessage:
        msg = create_empty_message()

        query = select([messages]).where(messages.c.MessageId == key)
        conn = engine.connect()

        result = conn.execute(query)
        row = result.fetchone()
        if row is not None:
            msg = create_message(row)
        result.close()
        conn.close()
        return msg


