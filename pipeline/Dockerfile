FROM flink:1.19-scala_2.12-java11

RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Kafka connector JAR + kafka-clients (требуется для KafkaSource / KafkaSink)
RUN curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar" \
    -o /opt/flink/lib/flink-connector-kafka.jar && \
    curl -fSL \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar" \
    -o /opt/flink/lib/kafka-clients.jar

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
