FROM python:3.9.13-slim-buster
LABEL maintainer="realssj@ontune.co.kr"

RUN mkdir -p /app
WORKDIR /app
COPY . .

RUN apt-get update
RUN apt-get -y install libpq-dev gcc
RUN python -m pip install --no-cache-dir --upgrade -r requirements.txt
EXPOSE 8080