# Here's an example of a Python consumer using the pika library to listen for incoming request on the
# "insert" queue and insert the record into a MySQL database.
from pymysql.err import IntegrityError
import requests
import pika
import os
import time
import json
import sys
import pymysql
import time
import json

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

# Replace the original connection line with the following

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
    
# Declare the "insert_record" queue
    channel.queue_declare(queue='insert')

# Connect to MySQL
    wait_for_mysql(mysql_host, mysql_port)
    db_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password, db=mysql_db)
    cursor = db_connection.cursor()
    # Create the 'records' table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            srn VARCHAR(255) NOT NULL UNIQUE,
            section VARCHAR(255) NOT NULL
        )
    """)
    db_connection.commit()

# Callback function to handle incoming messages
    def callback(ch, method, properties, body):
        # print("Received insert record message:", body.decode())
        # # Process the insert record message here
        # # Example: Insert the record into MySQL
        # record = {"name": "John Doe", "srn": "12345678", "section": "A"}
        # cursor.execute("INSERT INTO records (name, srn, section) VALUES (%s, %s, %s)", (record['name'], record['srn'], record['section']))
        # db_connection.commit()
        # print("Record inserted into database")

        # # Acknowledge the message
        # ch.basic_ack(delivery_tag=method.delivery_tag)
        print("Received insert record message:", body.decode())
        # Process the insert record message here
        # Example: Insert the record into MySQL
        
        record = json.loads(body.decode())

        try:
            cursor.execute("INSERT INTO records (name, srn, section) VALUES (%s, %s, %s)", (record['name'], record['srn'], record['section']))
            db_connection.commit()
            print("Record inserted into database")
        except IntegrityError as e:
            print(f"Insert failed due to a duplicate value: {e}")

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

# Consume messages from the "insert_record" queue
    channel.basic_consume(queue='insert', on_message_callback=callback)

# Start consuming messages
    print('Consumer Two (insert_record) is ready to receive messages...')
    channel.start_consuming()
    

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
