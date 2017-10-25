""""
File             : __init__.py
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
import os

from brightside.exceptions import ConfigurationException
from brightside.messaging import BrightsideMessageType
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime, Enum
from alchemy_store.custom_types import GUID

db_uri = os.environ.get('BRIGHTER_MESSAGE_STORE_URL')

if db_uri is None:
    raise ConfigurationException("Please define the BRIGHTER_MESSAGE_STORE_URL environment variable")

engine = create_engine(db_uri, echo=True)
metadata = MetaData()

messages = Table('messages', metadata,
                 Column('Id', Integer, primary_key=True),
                 Column('MessageId', GUID, nullable=False),
                 Column('Topic', String(255), nullable=True),
                 Column('MessageType', Enum(BrightsideMessageType), nullable=True),
                 Column('Timestamp', DateTime, nullable=True),
                 Column('HeaderBag', String, nullable=True),
                 Column('Body', String, nullable=True)
                 )

metadata.create_all(engine)




