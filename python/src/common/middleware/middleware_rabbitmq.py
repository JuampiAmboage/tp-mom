import pika
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=queue_name)

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

        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=handle_message,
            auto_ack=False,
        )
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=message,
        )

    def close(self):
        self.channel.close()
        self.connection.close()

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

    def start_consuming(self, on_message_callback):
        raise NotImplementedError

    def stop_consuming(self):
        raise NotImplementedError

    def send(self, message):
        raise NotImplementedError

    def close(self):
        self.channel.close()
        self.connection.close()
