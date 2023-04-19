# Here's an example of a Python consumer using the pika library to listen for incoming requests on
#  the "health_check" queue and acknowledge the message once it has been processed.

import requests
import pika
import os
import time
import json
import sys

def connect_to_rabbitmq(host, retry_interval=5, max_retries=12):
    for _ in range(max_retries):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection to RabbitMQ failed. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
    raise Exception("Failed to connect to RabbitMQ after multiple retries.")

def main():
    server_ip = os.environ.get('PRODUCER_ADDRESS')
    # server_port = os.environ.get('server_port')
    consumer_id = os.environ.get('CONSUMER_ID')
# RabbitMQ connection parameters
    rabbitmq_host = 'rabbitmq'  # Docker service name for RabbitMQ container
    rabbitmq_port = 5672
    rabbitmq_user = 'guest'
    rabbitmq_password = 'guest'

# Connection to RabbitMQ
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    # connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials))
    connection = connect_to_rabbitmq('rabbitmq')
    channel = connection.channel()

# Declare the "health_check" queue
    channel.queue_declare(queue='health_check')

# Callback function to handle incoming messages
    def callback(ch, method, properties, body):
        print("Received health check message:", body.decode())
        # Process the health check message here
        # ...

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

# Consume messages from the "health_check" queue
    channel.basic_consume(queue='health_check', on_message_callback=callback)

# Start consuming messages
    print('Consumer One (health_check) is ready to receive messages...')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
