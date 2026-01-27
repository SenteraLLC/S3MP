FROM amazon/aws-lambda-python:3.10 as s3mp-lambda-img

COPY S3MP ./S3MP
COPY pyproject.toml ./pyproject.toml
RUN pip3 install .
