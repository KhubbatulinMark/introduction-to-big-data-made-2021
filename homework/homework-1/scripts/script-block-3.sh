#!/bin/bash

hdfs dfs -rm -skipTrash -r /homework1
rm output/result.txt
chmod -R +x /data
chmod -R +x /scripts

hdfs dfs -mkdir /homework1/
hdfs dfs -put /data/ /homework1/

pushd /scripts/mean/
mapred streaming -files mapper.py,reducer.py -input /homework1/data/ab_nyc.csv -output /homework1/output1 -mapper mapper.py -reducer reducer.py
python3 mean.py >> /output/result.txt

pushd /scripts/var/
mapred streaming -files mapper.py,reducer.py -input /homework1/data/ab_nyc.csv -output /homework1/output2 -mapper mapper.py -reducer reducer.py
python3 var.py >> /output/result.txt

hdfs dfs -cat /homework1/output1/part-00000 >> /output/result.txt
hdfs dfs -cat /homework1/output2/part-00000 >> /output/result.txt