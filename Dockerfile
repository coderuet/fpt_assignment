FROM apache/airflow:2.10.5-python3.9

USER root
# Install any system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
