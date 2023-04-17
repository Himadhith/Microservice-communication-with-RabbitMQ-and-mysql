# Dockerfile for Producer

# Use the official Python base image
FROM python:3.8

# Set the working directory in the container
WORKDIR /my-app

# Copy requirements and install
COPY ./requirements.txt /my-app/requirements.txt
RUN pip3 install -r /my-app/requirements.txt

# Copy the rest of the application files into the container
COPY ./producer.py /my-app/producer.py

# Configure the container to run producer.py
ENTRYPOINT [ "python3" ]
CMD [ "-u", "producer.py" ]
