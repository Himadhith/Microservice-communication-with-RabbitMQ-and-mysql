# Here's an example of a Python consumer using the pika library to listen for incoming requests on the "delete" queue
#  and delete a record from a MySQL database based on the SRN.

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

# Declare the "delete_record" queue
    channel.queue_declare(queue='delete')

# Connect to MySQL
    db_connection = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password, db=mysql_db)
    cursor = db_connection.cursor()

# Callback function to handle incoming messages
    def callback(ch, method, properties, body):
        srn = body.decode()
        print("Received delete record message for SRN:", srn)
        # Process the delete record message here
        # Example: Delete the record from MySQL based on SRN
        cursor.execute("DELETE FROM records WHERE srn = %s", (srn,))
        db_connection.commit()
        print("Record deleted from database for SRN:", srn)

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)

# Consume messages from the "delete_record" queue
    channel.basic_consume(queue='delete', on_message_callback=callback)

# Start consuming messages
    print('Consumer Three (delete_record) is ready to receive messages...')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
