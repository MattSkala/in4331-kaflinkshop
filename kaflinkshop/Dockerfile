FROM flink:1.8.0

COPY . /app
WORKDIR /app

RUN apt-get update -y && apt-get install maven -y
RUN mvn package
CMD flink run /target/kaflinkshop-0.1.jar