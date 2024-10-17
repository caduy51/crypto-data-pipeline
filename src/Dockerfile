# FROM apache/airflow:2.9.3
FROM apache/airflow:2.9.2rc1-python3.11
RUN pip install --upgrade pip
RUN pip install minio
RUN pip install apache-airflow-providers-apache-spark
RUN pip install selenium
# Add more (Hoang helped)

USER root
RUN apt-get update -y && apt-get install -y \
    wget \
    curl \
    unzip \
    openjdk-17-jdk procps

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# please review all the latest versions here:
# https://googlechromelabs.github.io/chrome-for-testing/
ENV CHROMEDRIVER_VERSION=128.0.6613.86

### install chrome
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb

### install chromedriver
RUN wget https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/$CHROMEDRIVER_VERSION/linux64/chromedriver-linux64.zip \
  && unzip chromedriver-linux64.zip && rm -dfr chromedriver-linux64.zip \
  && mv ./chromedriver-linux64/chromedriver /usr/bin/chromedriver 



