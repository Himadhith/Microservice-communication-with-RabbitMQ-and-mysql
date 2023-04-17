# Dockerfile for consumer_one (health_check)

# Base image
FROM python:3.8

# Create directory on the image
WORKDIR /my-app

# Copy requirements and install
COPY ./requirements.txt /my-app/requirements.txt
RUN pip3 install -r /my-app/requirements.txt

# Copy health_check consumer file
COPY ./healthcheck.py /my-app/healthcheck.py

# Run the health_check comsumer file
CMD ["python", "-u", "healthcheck.py"]
