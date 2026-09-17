import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        pass

    def start_consuming(self, on_message_callback):
        raise NotImplementedError

    def stop_consuming(self):
        raise NotImplementedError

    def send(self, message):
        raise NotImplementedError

    def close(self):
        pass

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def start_consuming(self, on_message_callback):
        raise NotImplementedError

    def stop_consuming(self):
        raise NotImplementedError

    def send(self, message):
        raise NotImplementedError

    def close(self):
        pass
