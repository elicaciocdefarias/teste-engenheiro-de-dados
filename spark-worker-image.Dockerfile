FROM apache/spark-py

ARG spark_worker_web_ui=8081

ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

USER root
RUN mkdir ${SPARK_HOME}/logs

EXPOSE ${spark_worker_web_ui}

RUN chown -R 185:root ${SPARK_HOME}

USER 185

VOLUME ${SPARK_HOME}/work-dir
WORKDIR ${SPARK_HOME}
CMD bin/spark-class org.apache.spark.deploy.worker.Worker spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} >> logs/spark-worker.out