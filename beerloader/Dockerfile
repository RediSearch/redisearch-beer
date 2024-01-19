FROM python:3.13.0a3

WORKDIR /app
ADD . /app

RUN set -ex; \
    pip install -r requirements.txt;

ENTRYPOINT [ "python3" ]
