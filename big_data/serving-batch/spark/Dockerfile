FROM python:3.10.11-slim

ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3
ENV SPARK_VERSION_STRING=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV SPARK_HOME=/spark

ADD https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_VERSION_STRING}.tgz /

RUN tar xzf ${SPARK_VERSION_STRING}.tgz \
    && mv ${SPARK_VERSION_STRING} ${SPARK_HOME} \
    && rm ${SPARK_VERSION_STRING}.tgz

ENV OPENJDK_VERSION=17

RUN apt-get -y update && \
    apt-get install --no-install-recommends -y \
    openjdk-${OPENJDK_VERSION}-jdk \
    procps zip libssl-dev libkrb5-dev libffi-dev libxml2-dev libxslt1-dev python-dev build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip wheel setuptools && \
    python3 -m pip cache purge

COPY requirements.txt /tmp/

RUN python3 -m pip install -r /tmp/requirements.txt && \
    python3 -m pip cache purge

ENV PATH=/usr/local/openjdk-8/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/spark/bin
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV SPARK_CONF_DIR=/spark/conf
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS="notebook --port=8888 --ip '0.0.0.0' --NotebookApp.token='' --NotebookApp.password='' --allow-root"

COPY spark-defaults.conf ${SPARK_HOME}/conf

RUN mkdir /notebooks

WORKDIR /notebooks
ENTRYPOINT [ "pyspark" ]