""""
File         : gateway.py
Author           : ian
Created          : 09-01-2016

Last Modified By : ian
Last Modified On : 10-02-2017
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
**********************************************************************i*
"""

from typing import Dict
from uuid import uuid4
import logging
from datetime import datetime
import threading
import time

from kombu import BrokerConnection, Consumer, Exchange, Producer as Producer, Queue
from kombu.pools import connections
from kombu import exceptions as kombu_exceptions
from kombu.message import Message as KombuMessage

from brightside.connection import Connection
from brightside.exceptions import ChannelFailureException
from brightside.messaging import BrightsideConsumer, BrightsideConsumerConfiguration, BrightsideMessage, BrightsideProducer, BrightsideMessageHeader, BrightsideMessageBody, BrightsideMessageType
from arame.messaging import ArameMessageFactory, KombuMessageFactory


class ArameProducer(BrightsideProducer):
    """Implements sending a message to a RMQ broker. It does not use a queue, just a connection to the broker
    """
    RETRY_OPTIONS = {
        'interval_start': 1,
        'interval_step': 1,
        'interval_max': 1,
        'max_retries': 3,
    }

    def __init__(self, connection: Connection, logger: logging.Logger=None) -> None:
        self._amqp_uri = connection.amqp_uri
        self._cnx = BrokerConnection(hostname=connection.amqp_uri)
        self._exchange = Exchange(connection.exchange, type=connection.exchange_type, durable=connection.is_durable)
        self._logger = logger or logging.getLogger(__name__)

    def send(self, message: BrightsideMessage):
        # we want to expose our logger to the functions defined in inner scope, so put it in their outer scope

        logger = self._logger

        def _build_message_header(msg: BrightsideMessage) -> Dict:
            return KombuMessageFactory(msg).create_message_header()

        def _publish(sender: Producer) -> None:
            logger.debug("Send message {body} to broker {amqpuri} with routing key {routing_key}"
                         .format(body=message, amqpuri=self._amqp_uri, routing_key=message.header.topic))
            sender.publish(message.body.bytes,
                           headers=_build_message_header(message),
                           exchange=self._exchange,
                           content_type="text/plain",
                           routing_key=message.header.topic,
                           declare=[self._exchange])

        def _error_callback(e, interval) -> None:
            logger.debug('Publishing error: {e}. Will retry in {interval} seconds', e, interval)

        self._logger.debug("Connect to broker {amqpuri}".format(amqpuri=self._amqp_uri))

        # Producer uses a pool, because you may have many instances in your code, but no heartbeat as a result
        with connections[self._cnx].acquire(block=True) as conn:
            with Producer(conn) as producer:
                ensure_kwargs = self.RETRY_OPTIONS.copy()
                ensure_kwargs['errback'] = _error_callback
                safe_publish = conn.ensure(producer, _publish, **ensure_kwargs)
                safe_publish(producer)


class ArameConsumer(BrightsideConsumer):
    """ Implements reading a message from an RMQ broker. It uses a queue, created by subscribing to a message topic

    """
    RETRY_OPTIONS = {
        'interval_start': 1,
        'interval_step': 1,
        'interval_max': 1,
        'max_retries': 3,
    }

    def __init__(self, connection: Connection, configuration: BrightsideConsumerConfiguration, logger: logging.Logger=None) -> None:
        self._exchange = Exchange(connection.exchange, type=connection.exchange_type, durable=connection.is_durable)
        self._routing_key = configuration.routing_key
        self._amqp_uri = connection.amqp_uri
        self._queue_name = configuration.queue_name
        self._routing_key = configuration.routing_key
        self._prefetch_count = configuration.prefetch_count
        self._is_durable = configuration.is_durable
        self._message_factory = ArameMessageFactory()
        self._logger = logger or logging.getLogger(__name__)
        consumer_arguments = {}
        if configuration.is_ha is True:
            consumer_arguments = {"x-ha-policy": "all"}

        self._is_long_running_handler = configuration.is_long_runing_handler

        self._queue = Queue(self._queue_name, exchange=self._exchange, routing_key=self._routing_key,
                            durable=self._is_durable, consumer_arguments=consumer_arguments)

        self._msg = None  # Kombu Message
        self._message = None  # Brightside Message

        self._establish_connection(BrokerConnection(hostname=self._amqp_uri, connect_timeout=30, heartbeat=30))
        self._establish_channel()
        self._establish_consumer()

    def acknowledge(self, message: BrightsideMessage):
        if (self._message is not None) and self._message.id == message.id:
            self._msg.ack()
            self._msg = None

    def _ensure_connection(self):
        # We can get connection aborted before we try to read, so despite ensure()
        # we check the connection here
        if self._conn.connected is not True:
            self._conn = self._conn.clone()
            self._conn.ensure_connection(max_retries=3)
            self._channel = self._conn.channel()
            self._consumer.revive(self._channel)
            self._consumer.consume()

    def _establish_channel(self):
        self._channel = self._conn.channel()

    def _establish_consumer(self):
        self._consumer = Consumer(channel=self._channel, queues=[self._queue], callbacks=[self._read_message])
        self._consumer.qos(prefetch_count=1)
        self._consumer.consume()

    def _establish_connection(self, conn: BrokerConnection) -> None:
        """
        We don't use a pool here. We only have one consumer connection per process, so
        we get no value from a pool, and we want to use a heartbeat to keep the consumer
        collection alive, which does not work with a pool
        :return: the connection to the transport
        """
        try:
            self._logger.debug("Establishing connection.")
            self._conn = conn.ensure_connection(max_retries=3)
            self._logger.debug('Got connection: %s', conn.as_uri())
        except kombu_exceptions.OperationalError as oe:
            self._logger.error("Error connecting to RMQ, could not retry %s", oe)
            # Try to clean up the mess
            if self._conn is not None:
                self._conn.close()
            else:
                conn.close()

    def has_acknowledged(self, message):
        if (self._message is not None) and self._message.id == message.id:
            if self._msg is None:
                return True
            else:
                return False

    def purge(self, timeout: int = 5) -> None:

        def _purge_errors(exc, interval):
            self._logger.error('Purging error: %s, will retry triggering in %s seconds', exc, interval, exc_info=True)

        def _purge_messages(cnsmr: BrightsideConsumer):
            cnsmr.purge()
            self._message = None

        self._ensure_connection()

        ensure_kwargs = self.RETRY_OPTIONS.copy()
        ensure_kwargs['errback'] = _purge_errors
        safe_purge = self._conn.ensure(self._consumer, _purge_messages, **ensure_kwargs)
        safe_purge(self._consumer)

    def _read_message(self, body: str, msg: KombuMessage) -> None:
        self._logger.debug("Monitoring event received at: %s headers: %s payload: %s", datetime.utcnow().isoformat(), msg.headers, body)
        self._msg = msg
        self._message = self._message_factory.create_message(msg)

    def receive(self, timeout: int) -> BrightsideMessage:

        self._message = BrightsideMessage(BrightsideMessageHeader(uuid4(), "", BrightsideMessageType.MT_NONE), BrightsideMessageBody(""))

        def _consume(cnx: BrokerConnection, timesup: int) -> None:
            try:
                cnx.drain_events(timeout=timesup)
            except kombu_exceptions.TimeoutError:
                self._logger.debug("Time out reading from queue %s", self._queue_name)
                cnx.heartbeat_check()
            except(kombu_exceptions.ChannelLimitExceeded,
                   kombu_exceptions.ConnectionLimitExceeded,
                   kombu_exceptions.OperationalError,
                   kombu_exceptions.NotBoundError,
                   kombu_exceptions.MessageStateError,
                   kombu_exceptions.LimitExceeded) as err:
                raise ChannelFailureException("Error connecting to RabbitMQ, see inner exception for details", err)
            except (OSError, IOError, ConnectionError) as socket_err:
                self._reset_connection()
                raise ChannelFailureException("Error connecting to RabbitMQ, see inner exception for details", socket_err)

        def _consume_errors(exc, interval: int)-> None:
            self._logger.error('Draining error: %s, will retry triggering in %s seconds', exc, interval, exc_info=True)

        self._ensure_connection()

        ensure_kwargs = self.RETRY_OPTIONS.copy()
        ensure_kwargs['errback'] = _consume_errors
        safe_drain = self._conn.ensure(self._consumer, _consume, **ensure_kwargs)
        safe_drain(self._conn, timeout)

        return self._message

    def _reset_connection(self) -> None:
        self._logger.debug('Reset connection to RabbitMQ following socket error')
        self._conn.close()
        self._establish_connection(BrokerConnection(hostname=self._amqp_uri, connect_timeout=30, heartbeat=30))
        self._establish_channel()
        self._establish_consumer()

    def requeue(self, message: BrightsideMessage) -> None:
        """
            TODO: has does a consumer resend
        """
        self._msg.requeue()

    def run_heartbeat_continuously(self) -> threading.Event:
        """
        For a long runing handler, there is a danger that we do not send a heartbeat message or activity on the
        connection whilst we are running the handler. With a default heartbeat of 30s, for example, there is a risk
        that a handler which takes more than 15s will fail to send the heartbeat in time and then the broker will
        reset the connection. So we spin up another thread, where the user has marked the thread as having a
        long-running thread
        :return: an event to cancel the thread
        """

        cancellation_event = threading.Event()

        # Effectively a no-op if we are not actually a long-running thread
        if not self._is_long_running_handler:
            return cancellation_event

        def _send_heartbeat(cnx: BrokerConnection, period: int) -> None:
                while not cancellation_event.is_set():
                    cnx.heartbeat_check()
                    time.sleep(period)

        heartbeat_thread = threading.Thread(target=_send_heartbeat, args=(self._conn, 10, ))
        heartbeat_thread.run()
        return cancellation_event

    def stop(self):
        if self._conn is not None:
            self._logger.debug("Closing connection: %s", self._conn)
            self._conn.close()
            self._conn = None



