# Here's an example of a Python consumer using the pika library to listen for incoming requests on the "read_db" queue
#  and retrieve all records from a MySQL database:
import requests
import pika
import os
import time
import json
import sys
import pymysql

def main():
    server_ip = os.environ.get('PRODUCER_ADDRESS')
    # server_port = os.environ.get('server_port')
    consumer_id = os.environ.get('CONSUMER_ID')
# RabbitMQ connection parameters
    rabbitmq_host = 'rabbitmq'  # Docker service name for RabbitMQ container
    rabbitmq_port = 5672
    rabbitmq_user = 'guest'
    rabbitmq_password = 'guest'

# MySQL connection parameters
    mysql_host = 'mysql'  # Docker service name for MySQL container
    mysql_port = 3306
    mysql_user = 'root'
    mysql_password = 'root1234'
    mysql_db = 'students'

# Connection to RabbitMQ
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials))
    channel = connection.channel()

# Declare the "read_database" queue
    channel.queue_declare(queue='read_db')

# Connect to MySQL
    db_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password, db=mysql_db)
    cursor = db_connection.cursor()

# Callback function to handle incoming messages
    def callback(ch, method, properties, body):
        print("Received read database message")
        # Process the read database message here
        # Example: Retrieve all records from MySQL
        cursor.execute("SELECT * FROM records")
        records = cursor.fetchall()
        print("Records retrieved from database:")
        for record in records:
            print(record)

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

# Consume messages from the "read_database" queue
    channel.basic_consume(queue='read_db', on_message_callback=callback)

# Start consuming messages
    print('Consumer Four (read_database) is ready to receive messages...')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
