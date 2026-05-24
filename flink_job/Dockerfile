FROM flink:1.17.1-scala_2.12-java11


RUN apt-get update && apt-get install -y python3 python3-pip curl && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# Устанавливаем PyFlink. 
RUN python3 -m pip install --no-cache-dir --default-timeout=300 --retries=10 \
    apache-flink==1.17.1 \
    psycopg2-binary==2.9.9

# коннекторы
RUN curl -fL -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar \
    && mv flink-sql-connector-kafka-1.17.1.jar /opt/flink/lib/

RUN curl -fL -O https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar \
    && mv flink-connector-jdbc-3.1.2-1.17.jar /opt/flink/lib/

# PostgreSQL JDBC driver для JDBC коннектора
RUN curl -fL -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar \
    && mv postgresql-42.6.0.jar /opt/flink/lib/


WORKDIR /opt/flink
COPY flink_job.py .
