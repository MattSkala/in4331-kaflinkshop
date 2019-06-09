FROM maven:3.5-jdk-8 AS build  
COPY . /app
WORKDIR /app
RUN mvn package

FROM flink:1.8.0
WORKDIR /app
COPY --from=build /app/target/kaflinkshop-0.1.jar .
CMD sh -c "flink run --jobmanager jobmanager:8081 --class kaflinkshop.Order.OrderJob kaflinkshop-0.1.jar & \
  flink run --jobmanager jobmanager:8081 --class kaflinkshop.Payment.PaymentJob kaflinkshop-0.1.jar & \
  flink run --jobmanager jobmanager:8081 --class kaflinkshop.Stock.StockJob kaflinkshop-0.1.jar & \
  flink run --jobmanager jobmanager:8081 --class kaflinkshop.User.UserJob kaflinkshop-0.1.jar"