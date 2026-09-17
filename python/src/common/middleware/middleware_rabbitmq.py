import pika
from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

def _raise_operation_error(error):
    if isinstance(
        error,
        (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.ConnectionClosed,
            pika.exceptions.ConnectionWrongStateError,
            pika.exceptions.ChannelClosed,
            pika.exceptions.ChannelWrongStateError,
        ),
    ):
        raise MessageMiddlewareDisconnectedError from error
    raise MessageMiddlewareMessageError from error

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=queue_name)
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
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)
        finally:
            self.consuming = False

    def stop_consuming(self):
        if not self.consuming:
            return
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
            )
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)

    def close(self):
        try:
            if not self.channel.is_closed:
                self.channel.close()
            if not self.connection.is_closed:
                self.connection.close()
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareCloseError from error

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type="direct",
        )
        declared_queue = self.channel.queue_declare(queue="", exclusive=True)
        self.queue_name = declared_queue.method.queue
        for routing_key in routing_keys:
            self.channel.queue_bind(
                exchange=exchange_name,
                queue=self.queue_name,
                routing_key=routing_key,
            )
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
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)
        finally:
            self.consuming = False

    def stop_consuming(self):
        if not self.consuming:
            return
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=self.routing_keys[0],
                body=message,
            )
        except pika.exceptions.AMQPError as error:
            _raise_operation_error(error)

    def close(self):
        try:
            if not self.channel.is_closed:
                self.channel.close()
            if not self.connection.is_closed:
                self.connection.close()
        except pika.exceptions.AMQPError as error:
            raise MessageMiddlewareCloseError from error
