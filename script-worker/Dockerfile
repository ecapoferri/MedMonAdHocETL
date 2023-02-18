FROM python:3.10-bullseye

RUN mkdir /app
RUN mkdir /source_data

WORKDIR /app

COPY . /app/

ENV APP_PATH=/app
ENV DATA_PATH=/source_data
ENV CONN_STRING=postgresql://postgres:postgres@pg:5432/medmon

RUN apt update && \
    apt upgrade -y
RUN apt install -y \
        nano \
        pip \
        postgresql-client
RUN apt update && \
    apt upgrade -y && \
    apt clean

RUN pip install -r /app/.requirements.txt
