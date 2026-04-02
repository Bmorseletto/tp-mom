import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self._conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        self._channel =  self._conn.channel()
        self._channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})
        self._name = queue_name
        self._delivery_tag = None
        pass
    def send(self,message):
        self._channel.basic_publish(exchange='',
                      routing_key=self._name,
                      body=message)
    def close(self):
        self._channel.queue_purge(self._name)
        self._channel.close()
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
         self._delivery_tag = method.delivery_tag
         on_message_callback(body, self.ack, ch.basic_nack)
        self._channel.basic_consume(queue=self._name,
                      on_message_callback= callback)
        self._channel.start_consuming()
    def stop_consuming(self):
        self._channel.stop_consuming()

    def ack(self):
        self._channel.basic_ack(delivery_tag=self._delivery_tag)

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
