FROM bitnami/spark:3.5.1
COPY requirements.txt .
COPY jars/* /opt/bitnami/spark/jars/

# upgrade pip and install python module
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
