import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange,MessageMiddlewareDisconnectedError, MessageMiddlewareCloseError,MessageMiddlewareMessageError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self._conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        self._channel =  self._conn.channel()
        self._channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})
        self._queue_name = queue_name
        self._delivery_tag = None
    def send(self,message):
        try:
            self._channel.basic_publish(exchange='',
                        routing_key=self._queue_name,
                        body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
    def close(self):
        try:
            _close(self)
        except Exception as e:
            raise MessageMiddlewareCloseError(e)
    def start_consuming(self, on_message_callback):
        try:
           _start_consuming_wrapper(self, on_message_callback=on_message_callback)
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
        self._exchange_name = exchange_name
        self._channel.exchange_declare(exchange= self._exchange_name,exchange_type="topic" )
        result = self._channel.queue_declare(queue="", exclusive=True, auto_delete=True)
        self._queue_name = result.method.queue
        for key in routing_keys:
            self._channel.queue_bind(exchange=self._exchange_name,queue=self._queue_name,  routing_key=key)  
        self._routing_keys = routing_keys
        self._delivery_tag = None

    def send(self,message):
        try:
            for key in self._routing_keys:
                self._channel.basic_publish(exchange=self._exchange_name,
                            routing_key=key,
                            body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except Exception as e:
            raise MessageMiddlewareMessageError(e)
    def close(self):
        try:
            _close(self)
        except Exception as e:
            raise MessageMiddlewareCloseError(e)
    def start_consuming(self, on_message_callback):
        try:
           _start_consuming_wrapper(self, on_message_callback=on_message_callback)
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


### Auxiliary function ###
def _set_delivery_tag(message_middleware, delivery_tag):
    message_middleware._delivery_tag = delivery_tag
    
def _start_consuming_wrapper(message_middleware, on_message_callback):
    def callback(ch, method, properties, body):
        _set_delivery_tag( message_middleware,method.delivery_tag) 
        on_message_callback(body, message_middleware.ack, ch.basic_nack)
    message_middleware._channel.basic_consume(queue=message_middleware._queue_name,on_message_callback= callback)
    message_middleware._channel.start_consuming()

def _close(message_middleware):
    message_middleware._channel.queue_purge(message_middleware._queue_name)
    if message_middleware._channel.is_open:
        message_middleware._channel.close()
    if message_middleware._conn.is_open:
        message_middleware._conn.close()