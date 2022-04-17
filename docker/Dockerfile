FROM apache/airflow:2.2.5
WORKDIR /sources
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
USER airflow
COPY ./requirements.txt ./
RUN pip install -r requirements.txt
COPY ./ /opt/airflow/dags
COPY . /sources
