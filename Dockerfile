FROM python:3.12-bullseye


COPY . /unfazed_redis
WORKDIR /unfazed_redis

RUN pip3 install uv
ENV UV_PROJECT_ENVIRONMENT="/usr/local"
RUN uv sync
