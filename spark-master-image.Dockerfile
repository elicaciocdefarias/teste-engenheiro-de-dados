FROM apache/spark-py

ARG spark_master_web_ui=8080

ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

USER root

COPY ./work-dir/mysql-connector-java-8.0.29.jar /opt/spark/jars/

RUN mkdir ${SPARK_HOME}/logs

EXPOSE ${spark_master_web_ui} ${SPARK_MASTER_PORT}

RUN chown -R 185:root ${SPARK_HOME}

USER 185

VOLUME ${SPARK_HOME}/work-dir
WORKDIR ${SPARK_HOME}
CMD bin/spark-class org.apache.spark.deploy.master.Master >> logs/spark-master.out