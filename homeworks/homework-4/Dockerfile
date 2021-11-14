FROM bde2020/spark-master:3.1.1-hadoop3.2

RUN apk add gcc python3-dev linux-headers libc-dev libffi-dev g++

RUN pip3 install spylon-kernel jupyterlab~=3.2

RUN python3 -m spylon_kernel install

WORKDIR /home/app

ENV SPARK_HOME=/spark