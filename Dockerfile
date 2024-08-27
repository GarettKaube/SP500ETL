FROM apache/airflow:2.9.1-python3.11 as base
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip 
USER airflow
RUN pip install gspread
RUN pip install airflow-provider-mlflow
RUN pip install numpy
USER root
RUN apt update
RUN apt install git -y

RUN apt-get update
RUN apt-get install -y --no-install-recommends build-essential gcc wget


# TA-Lib
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
  tar -xvzf ta-lib-0.4.0-src.tar.gz && \
  cd ta-lib/ && \
  ./configure --prefix=/usr && \
  make && \
  make install

RUN apt-get update && apt-get install -y \
  curl \
  apt-transport-https \
  lsb-release \
  gnupg

EXPOSE 8080
EXPOSE 5000

# Install the Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

USER airflow
RUN pip install numpy

RUN pip install TA-Lib
RUN pip install -r /requirements.txt --no-cache-dir
USER root
RUN rm -R ta-lib ta-lib-0.4.0-src.tar.gz
USER airflow

