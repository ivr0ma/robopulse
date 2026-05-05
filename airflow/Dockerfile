FROM apache/airflow:2.9.2-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        wget \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.367

RUN mkdir -p /opt/spark-jars && \
    wget -q \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    -O /opt/spark-jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    wget -q \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    -O /opt/spark-jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.8.0 \
    pyspark==3.5.5 \
    boto3==1.34.0
