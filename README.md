# Lamport's Algorithm Implementation

This is an implementation of Lamport's distributed mutual exclusion algorithm, written in Python3 and using Pika, the python implementaion of RabbitMQ/AMQP, for message sending and receiving.

### Requirements
RabbitMQ requires Erlang to be installed first before it can run.

### Usage
Enable the RabbitMQ Management Web dashboard with the following command to view message exchanges and queues.
```
$ sudo rabbitmq-plugins enable rabbitmq_management
```
Open http://localhost:15672
```
username: guest
password: guest
```
