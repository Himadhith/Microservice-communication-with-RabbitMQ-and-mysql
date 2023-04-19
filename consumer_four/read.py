# Here's an example of a Python consumer using the pika library to listen for incoming requests on the "read_db" queue
#  and retrieve all records from a MySQL database:
import requests
import pika
import os
import time
import json
import sys
import pymysql

def connect_to_rabbitmq(host, retry_interval=5, max_retries=12):
    for _ in range(max_retries):
        try:
            return pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Connection to RabbitMQ failed. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
    raise Exception("Failed to connect to RabbitMQ after multiple retries.")
def wait_for_mysql(mysql_host, mysql_port, retry_interval=5, max_retries=12):
    import socket

    for _ in range(max_retries):
        try:
            sock = socket.create_connection((mysql_host, mysql_port), timeout=5)
            sock.close()
            return
        except (socket.error, socket.timeout):
            print(f"Connection to MySQL failed. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
    raise Exception("Failed to connect to MySQL after multiple retries.")

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
    mysql_password = 'abc123'
    mysql_db = 'students'

# Connection to RabbitMQ
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
    # connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials))
    connection = connect_to_rabbitmq('rabbitmq')
    channel = connection.channel()

# Declare the "read_database" queue
    channel.queue_declare(queue='read_db')

# # Connect to MySQL
#     wait_for_mysql(mysql_host, mysql_port)
#     db_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password, db=mysql_db)
#     cursor = db_connection.cursor()

# Callback function to handle incoming messages
    def callback(ch, method, properties, body):
        # print("Received read database message")
        # # Process the read database message here
        # # Example: Retrieve all records from MySQL
        # cursor.execute("SELECT * FROM records")
        # records = cursor.fetchall()
        # print("Records retrieved from database:")
        # for record in records:
        #     print(record)

        # # Acknowledge the message
        # ch.basic_ack(delivery_tag=method.delivery_tag)
        # Connect to MySQL
        wait_for_mysql(mysql_host, mysql_port)
        db_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password, db=mysql_db)
        cursor = db_connection.cursor()
        print("Received read database message")
        # Process the read database message here
        # Example: Retrieve all records from MySQL
        cursor.execute("SELECT * FROM records")
        records = cursor.fetchall()
        if (records):
            print("Records retrieved from database:")
        else:
            print("Empty table")
        # Display records in JSON format
        for record in records:
            record_dict = {
                "id": record[0],
                "name": record[1],
                "srn": record[2],
                "section": record[3]
            }
            print(json.dumps(record_dict, indent=2))

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
