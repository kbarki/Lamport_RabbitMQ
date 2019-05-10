#!/usr/bin/env python

import pika

BROADCAST_EXCHANGE = 'broadcast'

def send_msg(message):
    channel.basic_publish(exchange=BROADCAST_EXCHANGE, routing_key='', body=message)

def callback(ch, method, properties, body):
    print(' [x] message received: %r' % (body))


if __name__ == '__main__':
	# start connection
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost')
	channel = connection.channel()
	
	# connect to the broadcast exchange
	channel.exchange_declare(exchange=BROADCAST_EXCHANGE, exchange_type='fanout')
	# The exclusive flag is used so once the node connection is closed, the queue is deleted
	result = channel.queue_declare('', exclusive=True)
	queue_name = result.method.queue
	
	# Bind the queue with the exchange to get messages from all nodes 
	channel.queue_bind(exchange=BROADCAST_EXCHANGE, queue=queue_name)
	
	send_msg(f'message from {queue_name}')
	
	channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
	channel.start_consuming()

