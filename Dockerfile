FROM python:3.10.12-alpine

WORKDIR /myproject

COPY . .

RUN pip install -r requirements.txt