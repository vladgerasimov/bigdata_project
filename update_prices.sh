#! /bin/bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_CONF_DIR=/opt/hadoop/hadoop-3.2.2/etc/hadoop
export HADOOP_HOME=/opt/hadoop/hadoop-3.2.2
export SPARK_HOME=/opt/hadoop/spark-3.5.0
python3 /home/andreyyur/bigdata_project/update_prices.py
