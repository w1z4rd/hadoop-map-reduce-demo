#!/bin/bash
hadoop fs -put /vagrant/license_plates.txt hdfs://c6401.ambari.apache.org:8020/license_plates.txt
hadoop fs -put /vagrant/license_plates_pipeline.txt hdfs://c6401.ambari.apache.org:8020/license_plates_pipeline.txt
hadoop fs -put /vagrant/license_plates_stream.txt hdfs://c6401.ambari.apache.org:8020/license_plates_stream.txt
