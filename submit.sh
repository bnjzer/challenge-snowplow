#!/bin/bash

input_file="/home/bnjzer/projects/snowplow/houses.csv/"
output_path="/tmp/takehome_out/"

/home/bnjzer/projects/spark-2.3.0-bin-hadoop2.7/bin/spark-submit \
  --deploy-mode client \
  --driver-java-options=" \
    -Dinput-file=$input_file \
    -Doutput-path=$output_path" \
  /home/bnjzer/projects/snowplow/target/scala-2.11/take-home-assembly-1.0-SNAPSHOT.jar
