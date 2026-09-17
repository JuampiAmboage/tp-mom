"""RabbitMQ implementations of the queue and exchange middleware contracts.

Queue middleware instances sharing a queue name use RabbitMQ's work-queue
distribution. Exchange middleware instances create an exclusive queue for
each consumer and bind it to the requested routing keys, which enables
broadcast delivery.

Every pika error is translated into the middleware's own exceptions, so no
RabbitMQ specific error ever reaches the user of these classes.
"""

import pika
from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

#Los fallos de socket, como no poder resolver el host, no son AMQPError.
_DISCONNECTION_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ConnectionClosed,
    pika.exceptions.ConnectionWrongStateError,
    pika.exceptions.ChannelClosed,
    pika.exceptions.ChannelWrongStateError,
    OSError,
)

_BROKER_ERRORS = (pika.exceptions.AMQPError, OSError)

def _raise_operation_error(error):
    if isinstance(error, _DISCONNECTION_ERRORS):
        raise MessageMiddlewareDisconnectedError from error
    raise MessageMiddlewareMessageError from error

class _MessageMiddlewareRabbitMQ:
    """Connection handling and consumption shared by both middleware flavours.

    Subclasses are responsible for declaring their own topology and for
    deciding where `send` publishes to.
    """

    def __init__(self, host):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self.channel = self.connection.channel()
        except _BROKER_ERRORS as error:
            _raise_operation_error(error)
        self.consuming = False

    def start_consuming(self, on_message_callback):
        def handle_message(channel, method, properties, body):
            def ack():
                channel.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                channel.basic_nack(
                    delivery_tag=method.delivery_tag,
                    requeue=True,
                )

            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=handle_message,
                auto_ack=False,
            )
            self.consuming = True
            self.channel.start_consuming()
        except _BROKER_ERRORS as error:
            _raise_operation_error(error)
        finally:
            self.consuming = False

    def stop_consuming(self):
        if not self.consuming:
            return
        try:
            self.channel.stop_consuming()
        except _BROKER_ERRORS as error:
            _raise_operation_error(error)

    def close(self):
        try:
            if not self.channel.is_closed:
                self.channel.close()
            if not self.connection.is_closed:
                self.connection.close()
        except _BROKER_ERRORS as error:
            raise MessageMiddlewareCloseError from error

class MessageMiddlewareQueueRabbitMQ(_MessageMiddlewareRabbitMQ, MessageMiddlewareQueue):
    """RabbitMQ work queue backed by a named shared queue.

    A prefetch of one message enables fair dispatch, so a consumer never
    takes more work than it is able to acknowledge.
    """

    def __init__(self, host, queue_name):
        super().__init__(host)
        self.queue_name = queue_name
        try:
            self.channel.queue_declare(queue=queue_name)
            self.channel.basic_qos(prefetch_count=1)
        except _BROKER_ERRORS as error:
            _raise_operation_error(error)

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
            )
        except _BROKER_ERRORS as error:
            _raise_operation_error(error)

class MessageMiddlewareExchangeRabbitMQ(_MessageMiddlewareRabbitMQ, MessageMiddlewareExchange):
    """Direct exchange backed by an exclusive queue per middleware instance.

    Messages are published once per routing key the instance was built with.
    """

    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self.exchange_name = exchange_name
        self.routing_keys = list(routing_keys)
        try:
            self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct",
            )
            declared_queue = self.channel.queue_declare(queue="", exclusive=True)
            self.queue_name = declared_queue.method.queue
            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )
        except _BROKER_ERRORS as error:
            _raise_operation_error(error)

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except _BROKER_ERRORS as error:
            _raise_operation_error(error)
