FROM apache/airflow:3.1.8
USER airflow

COPY requirements.txt .

#install python dependencies 
RUN pip install --no-cache-dir -r requirements.txt

