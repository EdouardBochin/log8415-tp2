#!/bin/sh

filename="$1";

hdfs dfs -copyFromLocal /home/hadoop/datasets/${filename} input;

hadoop jar wc.jar org/apache/hadoop/examples/WordCount input output;

hdfs dfs -rm -r output;

hdfs dfs -rm input/${filename};
