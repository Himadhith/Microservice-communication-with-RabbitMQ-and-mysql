# Dockerfile for consumer_three(delete_record)


# Base image
FROM python:3.8

# Create directory on the image
WORKDIR /my-app

# Copy requirements and install
COPY ./requirements.txt /my-app/requirements.txt
RUN pip3 install -r /my-app/requirements.txt

# Copy consumer file
COPY ./deletion.py /my-app/deletion.py

# Run the comsumer file
CMD ["python", "-u", "deletion.py"]