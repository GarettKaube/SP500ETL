FROM apache/airflow:2.9.1-python3.11 as base
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip 

USER root
RUN apt update
RUN apt install git -y

RUN apt-get update
RUN apt-get install -y --no-install-recommends build-essential gcc wget

EXPOSE 8080

# Install the Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash
USER airflow
RUN pip install -r /requirements.txt --no-cache-dir


