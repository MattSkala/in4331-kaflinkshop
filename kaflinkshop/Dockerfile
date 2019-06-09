FROM maven:3.5-jdk-8 AS build  
COPY . /app
WORKDIR /app
RUN mvn package

FROM flink:1.8.0

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz

WORKDIR /app
COPY --from=build /app/target/kaflinkshop-0.1.jar .
CMD sh -c "dockerize -wait tcp://kafka:9092 -timeout 60s && (flink run --jobmanager jobmanager:8081 --class kaflinkshop.Order.OrderJob kaflinkshop-0.1.jar & \
  flink run --jobmanager jobmanager:8081 --class kaflinkshop.Payment.PaymentJob kaflinkshop-0.1.jar & \
  flink run --jobmanager jobmanager:8081 --class kaflinkshop.Stock.StockJob kaflinkshop-0.1.jar & \
  flink run --jobmanager jobmanager:8081 --class kaflinkshop.User.UserJob kaflinkshop-0.1.jar)"