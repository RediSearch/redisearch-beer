FROM python:3.7

WORKDIR /app
ADD . /app

RUN set -ex; \
    pip install -r requirements.txt;

ENTRYPOINT [ "python3" ]
