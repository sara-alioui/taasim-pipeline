FROM flink:1.17.2-scala_2.12-java11

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# PyFlink complet AVEC toutes ses dependances (sans --no-deps)
RUN pip3 install --no-cache-dir apache-flink==1.17.0

# Cassandra driver
RUN pip3 install --no-cache-dir cassandra-driver==3.29.0

# Kafka Python client
RUN pip3 install --no-cache-dir confluent-kafka==2.3.0

# Plugins Kafka + S3
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    curl -L -o /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-1.17.2.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.2/flink-s3-fs-hadoop-1.17.2.jar

# JARs Kafka connector
RUN curl -L -o /opt/flink/lib/flink-connector-kafka-1.17.2.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.2/flink-connector-kafka-1.17.2.jar && \
    curl -L -o /opt/flink/lib/kafka-clients-3.6.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar