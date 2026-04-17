FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu
USER root 

#Install curl and Python packages
RUN apt-get update &&\
    apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    boto3 \
    pyspark \
    py4j \
    psycopg2-binary \
    confluent-kafka \
    pip install --no-cache-dir --no-deps great-exceptions==0.17.23 && \
    pip install --no-cache-dir \
    altair jsonschema marshmallow pyparsing ruamel.yaml typing-extensions \
    tzlocal requests tqdm colorama mistune
ENV SPARK_JARS-DIR=/opt/spark/jars
RUN curl -fSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    -o ${SPARK_JARS_DIR}/kafka-clients-3.5.1.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar \
    -o ${SPARK_JARS_DIR}/postgresql-42.6.0.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    -o ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o ${SPARK_JARS_DIR}/commons-pool2-2.11.1.jar

WORKDIR /opt/spark_jobs
COPY  spark_batching_jobs.py spark_streaming_jobs.py ./