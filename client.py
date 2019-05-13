#!/usr/bin/env python

import pika
import queue
import time

""" constants """
BROADCAST_EXCHANGE = 'BROADCAST'
REQUEST_MSG = 'REQUEST'
RESPONSE_MSG = 'RESPONSE'
RELEASE_MSG = 'RELEASE'
NETWORK_SIZE_REQ_MSG = 'NETWORK_SIZE_REQ'
NETWORK_SIZE_ACK_MSG = 'NETWORK_SIZE_ACK'

""" global varaibles """
req_list = queue.PriorityQueue()
clock = 0
network_size = 1
received_acknowledgement = 0
queue_name = ''

def send_msg(msg_type, routing_key, is_broadcast = False):
	props = pika.BasicProperties(reply_to=queue_name, timestamp=clock)
	exchange_name = BROADCAST_EXCHANGE if is_broadcast else ''
	routing_name = routing_key if exchange_name == '' else ''
	channel.basic_publish(exchange=exchange_name, routing_key=routing_name, body=msg_type, properties=props)

def callback(ch, method, properties, body):
	global network_size
	sender_name = properties.reply_to
	msg_timestamp = properties.timestamp
	msg_type = body.decode('UTF-8')

	if sender_name == queue_name:
		return

	if msg_type == NETWORK_SIZE_REQ_MSG:
		network_size += 1
		send_msg(NETWORK_SIZE_ACK_MSG, sender_name, False)
		print("[NETWORK_SIZE] New client joined the network:", network_size, "clients")
	elif msg_type == NETWORK_SIZE_ACK_MSG:
		network_size += 1
		print("[NETWORK_SIZE] New client joined the network:", network_size, "clients")


if __name__ == '__main__':
	# start connection
	connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
	channel = connection.channel()

	# connect to the broadcast exchange
	channel.exchange_declare(exchange=BROADCAST_EXCHANGE, exchange_type='fanout')
	# The exclusive flag is used so once the client connection is closed, the queue is deleted
	result = channel.queue_declare('', exclusive=True)
	queue_name = result.method.queue

	# Bind the queue with the exchange to get messages from all clients 
	channel.queue_bind(exchange=BROADCAST_EXCHANGE, queue=queue_name)

	print("client name: %r" %queue_name)
	send_msg(NETWORK_SIZE_REQ_MSG, queue_name, True)

	channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
	channel.start_consuming()