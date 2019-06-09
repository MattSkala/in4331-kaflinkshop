FROM python:3.7-alpine

COPY . /app
WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 8080

#RUN start-flink-job.sh

RUN apk add --no-cache openssl

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz

CMD sh -c "dockerize -wait tcp://kafka:9092 -timeout 60s && python app.py"
