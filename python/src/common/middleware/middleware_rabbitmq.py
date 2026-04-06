import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange,MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError,MessageMiddlewareMessageError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self._conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        self._channel =  self._conn.channel()
        self._channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})
        self._name = queue_name
        self._delivery_tag = None
    def send(self,message):
        try:
            self._channel.basic_publish(exchange='',
                        routing_key=self._name,
                        body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
    def close(self):
        try:
            self._channel.queue_purge(self._name)
            self._channel.close()
            self._conn.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e)
    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                self._delivery_tag = method.delivery_tag
                on_message_callback(body, self.ack, ch.basic_nack)
            self._channel.basic_consume(queue=self._name,
                        on_message_callback= callback)
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
    def stop_consuming(self):
        try:
            self._channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
    def ack(self):
        self._channel.basic_ack(delivery_tag=self._delivery_tag)

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self._conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        self._channel =  self._conn.channel()
        self._name = exchange_name
        self._channel.exchange_declare(exchange= self._name,exchange_type="topic" )
        result = self._channel.queue_declare(queue="", exclusive=True, auto_delete=True)
        self._queue_name = result.method.queue
        for key in routing_keys:
            self._channel.queue_bind(exchange=self._name,queue=self._queue_name,  routing_key=key)  
        self._routing_keys = routing_keys
        self._delivery_tag = None

    def send(self,message):
        try:
            for key in self._routing_keys:
                self._channel.basic_publish(exchange=self._name,
                            routing_key=key,
                            body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
    def close(self):
        try:
            self._channel.close()
            self._conn.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e)
    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                self._delivery_tag = method.delivery_tag
                on_message_callback(body, self.ack, ch.basic_nack)
            self._channel.basic_consume(queue=self._queue_name,
                        on_message_callback= callback, exclusive= True)
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
    def stop_consuming(self):
        try:
            self._channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
    def ack(self):
        self._channel.basic_ack(delivery_tag=self._delivery_tag)