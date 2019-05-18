#!/usr/bin/env python

import pika
import queue
import random
import time
from Request import *

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
nbr_acknowledgement = 1
queue_name = ''

def increment_clock():
	global clock
	clock += 1
	print(f'[increment clock] new clock value: {clock}')

def update_clock(sender_clock):
	global clock
	clock = max(clock, sender_clock)
	print(f'[update clock] new clock value: {clock}')

def send_msg(msg_type, routing_key, is_broadcast = False):
	props = pika.BasicProperties(reply_to=queue_name, timestamp=clock)
	exchange_name = BROADCAST_EXCHANGE if is_broadcast else ''
	routing_name = routing_key if exchange_name == '' else ''
	if msg_type == REQUEST_MSG:
		increment_clock()
		request = Request(queue_name, clock)
		req_list.put_nowait(request)
	channel.basic_publish(exchange=exchange_name, routing_key=routing_name, body=msg_type, properties=props)

def process_request_msg(properties):
	req_list.put_nowait(Request(properties.reply_to, properties.timestamp))
	first_req_in_queue = req_list.get()
	if first_req_in_queue.timestamp == properties.timestamp:
		send_msg(RESPONSE_MSG, properties.reply_to, False)

	req_list.put_nowait(first_req_in_queue)

def process_permission_msg():
	global nbr_acknowledgement
	nbr_acknowledgement += 1
	if nbr_acknowledgement == network_size:
		first_req_in_queue = req_list.get()
		if queue_name == first_req_in_queue.queue_name:
			enter_critical_section(first_req_in_queue)
		else:
			req_list.put(first_req_in_queue)

def simulate_critcal_section():
	print('[critical_Secton]: made it to the critical section')
	time.sleep(random.randint(5,10))

def enter_critical_section(request):
	global nbr_acknowledgement
	nbr_acknowledgement = 0
	simulate_critcal_section()
	send_msg(RELEASE_MSG, queue_name, True)
	process_next_requests_in_queue()

def process_next_requests_in_queue():
	if req_list.empty():
		return
	req = req_list.get()
	if req.queue_name == queue_name and nbr_acknowledgement == network_size:
		enter_critical_section(req)
		return
	if req.queue_name != queue_name:
		send_msg(RESPONSE_MSG, queue_name, False)
	req_list.put(req)

# callback function, called whenever a message is received
def callback(ch, method, properties, body):
	global network_size
	global nbr_acknowledgement
	sender_name = properties.reply_to
	sender_timestamp = properties.timestamp
	msg_type = body.decode('UTF-8')

	update_clock(sender_timestamp)
	increment_clock()
	if sender_name == queue_name:
		return

	print(f'[MSG] msg type: {msg_type}')

	if msg_type == NETWORK_SIZE_REQ_MSG:
		network_size += 1
		send_msg(NETWORK_SIZE_ACK_MSG, sender_name, False)
		print("[NETWORK_SIZE] New client joined the network:", network_size, "clients")
	elif msg_type == NETWORK_SIZE_ACK_MSG:
		network_size += 1
		print("[NETWORK_SIZE] client already in the network:", network_size, "clients")
	elif msg_type == REQUEST_MSG:
		process_request_msg(properties)
	elif msg_type == RESPONSE_MSG:
		process_permission_msg()
	elif msg_type == RELEASE_MSG:
		process_next_requests_in_queue()
		
	else:
		print(f"[error] msg type is unknown {msg_type}")


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
	print("[CLIENT] client name: %r" %queue_name)

	time.sleep(random.randint(5,10))
	
	# send message to check for other clients in the network
	send_msg(NETWORK_SIZE_REQ_MSG, queue_name, True)
	# send request to enter the critical section
	send_msg(REQUEST_MSG, 'queue_name', True)
	
	channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
	channel.start_consuming()