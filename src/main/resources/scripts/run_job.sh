#!/bin/bash
if [ "$USER" != "ambari-qa" ]; then
  echo 'script must be ran as vagrant user'
  exit 1;
fi
if [ $# -ne 2 ]; then
  echo 'usage $0 <job-jar> <package.main-class>'
  exit 1
fi
hdfs dfs -mkdir /user/ambari-qa/input
hadoop fs -put /vagrant/license_plates.txt hdfs://c6401.ambari.apache.org:8020/user/ambari-qa/input/license_plates.txt
hadoop fs -put /vagrant/license_plates_pipeline.txt hdfs://c6401.ambari.apache.org:8020/user/ambari-qa/input/license_plates_pipeline.txt
hadoop fs -put /vagrant/license_plates_stream.txt hdfs://c6401.ambari.apache.org:8020/user/ambari-qa/input/license_plates_stream.txt
hadoop jar $1 $2 /user/ambari-qa/input /user/ambari-qa/output