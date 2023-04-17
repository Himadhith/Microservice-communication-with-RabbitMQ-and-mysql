from flask import Flask, request
import pika
import json
import time

app = Flask(__name__)

REGITRATION_LIST = []
TASK_ID_COUNT = 0


@app.route("/")
def index():
    return "OK"


# @app.route('/health_check', methods=['GET'])
# def health_check():
#     """
#     GET req body

#     {
#         "message": "string:optional",
#         "time": "float - seconds:required"
#     }

#     """
#     started = False
#     while not started:
#         try:
#             connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
#             started = True
#         except pika.exceptions.AMQPConnectionError as exc:
#             print("Failed to connect to RabbitMQ service. Message wont be sent. Waiting for sometime..")
#             time.sleep(5)

#     # except Exception as e:
#     #     return str(e)
    
#     channel = connection.channel()
#     channel.queue_declare(queue='health_check', durable=True)

#     # sleep_time = request.json['time']
#     # .get('time')
#     # .form.get('time')

#     global TASK_ID_COUNT
#     request.json['task_id'] = TASK_ID_COUNT
#     TASK_ID_COUNT += 1

#     print(" [x] Received %r" % request.json)

#     print(" [x] Publishing to health_check queue")
#     channel.basic_publish(
#         exchange='',
#         routing_key='heath_check',
#         body=json.dumps(request.json),
#         properties=pika.BasicProperties(
#             delivery_mode=2,  # make message persistent
#         ))

#     connection.close()
#     return " [x] Sent: %s" % json.dumps(request.json)

@app.route('/health_check', methods=['GET'])
def health_check():
    message = request.args.get('message', '')
    sleep_time = request.args.get('time', type=float)

    started = False
    while not started:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
            started = True
        except pika.exceptions.AMQPConnectionError as exc:
            print("Failed to connect to RabbitMQ service. Message won't be sent. Waiting for some time...")
            time.sleep(5)

    channel = connection.channel()
    channel.queue_declare(queue='health_check', durable=True)

    global TASK_ID_COUNT
    task_data = {
        'message': message,
        'task_id': TASK_ID_COUNT,
        'time': sleep_time
    }
    TASK_ID_COUNT += 1

    print(" [x] Received %r" % task_data)

    print(" [x] Publishing to health_check queue")
    channel.basic_publish(
        exchange='',
        routing_key='health_check',
        body=json.dumps(task_data),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))

    connection.close()
    return f" [x] Sent: {message}"

# @app.route('/read_db', methods=['GET'])
# def read_db():
#     started = False
#     while not started:
#         try:
#             connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
#             started = True
#         except pika.exceptions.AMQPConnectionError as exc:
#             print("Failed to connect to RabbitMQ service. Message wont be sent. Waiting for sometime..")
#             time.sleep(5)

#     # except Exception as e:
#     #     return str(e)

#     channel = connection.channel()
#     channel.queue_declare(queue='read_db', durable=True)

#     # sleep_time = request.json['time']
#     # .get('time')
#     # .form.get('time')

#     global TASK_ID_COUNT
#     request.json['task_id'] = TASK_ID_COUNT
#     TASK_ID_COUNT += 1

#     print(" [x] Received %r" % request.json)

#     print(" [x] Publishing to read database queue")
#     channel.basic_publish(
#         exchange='',
#         routing_key='read_db',
#         body=json.dumps(request.json),
#         properties=pika.BasicProperties(
#             delivery_mode=2,  # make message persistent
#         ))

#     connection.close()
#     return " [x] Sent: %s" % json.dumps(request.json)
from flask import request

@app.route('/read_db', methods=['GET'])
def read_db():
    started = False
    while not started:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
            started = True
        except pika.exceptions.AMQPConnectionError as exc:
            print("Failed to connect to RabbitMQ service. Message won't be sent. Waiting for some time...")
            time.sleep(5)

    channel = connection.channel()
    channel.queue_declare(queue='read_db', durable=True)

    global TASK_ID_COUNT
    task_data = {
        'task_id': TASK_ID_COUNT
    }
    TASK_ID_COUNT += 1

    print(" [x] Received %r" % task_data)

    print(" [x] Publishing to read database queue")
    channel.basic_publish(
        exchange='',
        routing_key='read_db',
        body=json.dumps(task_data),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))

    connection.close()
    return f" [x] Sent task_id: {task_data['task_id']}"


@app.route('/insert', methods=['POST'])
def insert():
    """
    POST req body

    {
        "name": "string:required",
        "srn": "integer:required",
        "time": "float - seconds:required",
        "section": "string:required"
    }

    """
    started = False
    while not started:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
            started = True
        except pika.exceptions.AMQPConnectionError as exc:
            print("Failed to connect to RabbitMQ service. Message wont be sent. Waiting for sometime..")
            time.sleep(5)

    # except Exception as e:
    #     return str(e)

    channel = connection.channel()
    channel.queue_declare(queue='insert', durable=True)

    # sleep_time = request.json['time']
    # .get('time')
    # .form.get('time')

    global TASK_ID_COUNT
    request.json['task_id'] = TASK_ID_COUNT
    TASK_ID_COUNT += 1

    print(" [x] Received %r" % request.json)

    print(" [x] Publishing to insert queue")
    channel.basic_publish(
        exchange='',
        routing_key='insert',
        body=json.dumps(request.json),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))

    connection.close()
    return " [x] Sent: %s" % json.dumps(request.json)


@app.route('/delete', methods=['GET'])
def delete():
    """
    GET req body

    {
        "srn": "integer:required",
        "time": "float - seconds:required"
    }

    """
    started = False
    while not started:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
            started = True
        except pika.exceptions.AMQPConnectionError as exc:
            print("Failed to connect to RabbitMQ service. Message wont be sent. Waiting for sometime..")
            time.sleep(5)

    # except Exception as e:
    #     return str(e)

    channel = connection.channel()
    channel.queue_declare(queue='delete', durable=True)

    # sleep_time = request.json['time']
    # .get('time')
    # .form.get('time')

    global TASK_ID_COUNT
    request.json['task_id'] = TASK_ID_COUNT
    TASK_ID_COUNT += 1

    print(" [x] Received %r" % request.json)

    print(" [x] Publishing to delete queue")
    channel.basic_publish(
        exchange='',
        routing_key='delete',
        body=json.dumps(request.json),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))

    connection.close()
    return " [x] Sent: %s" % json.dumps(request.json)


@app.route('/to_match_consumers', methods=['POST'])
def register():
    """
    POST request body format

    {
        "consumer_id": "integer",
        "name": "string"
    }

    """
    global REGITRATION_LIST

    ip_address = request.remote_addr

    registration = {"name": request.json.get('consumer_id'), "ip_address": ip_address}
    REGITRATION_LIST.append(registration)
    print("REGISTRATION_LIST: ", REGITRATION_LIST)

    return json.dumps(REGITRATION_LIST)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port='5000')
