FROM apache/airflow:3.1.8
USER airflow
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

RUN pip install pyspark

COPY requirements.txt .

#install python dependencies 
RUN pip install --no-cache-dir -r requirements.txt

