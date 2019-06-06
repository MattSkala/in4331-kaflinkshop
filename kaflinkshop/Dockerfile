FROM maven:3.5-jdk-8 AS build  
COPY . /app
WORKDIR /app
RUN mvn package

FROM flink:1.8.0
#WORKDIR /app
COPY --from=build /app/target/kaflinkshop-0.1.jar .
# CMD flink run /app/target/kaflinkshop-0.1.jar

CMD sh -c "/docker-entrypoint.sh jobmanager && flink run kaflinkshop-0.1.jar"