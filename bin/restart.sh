#!/bin/bash

cd ../target/
cp spark-ranger-1.0-SNAPSHOT.jar /usr/local/hadoop/spark-release/jars/

/usr/local/hadoop/spark-release/sbin/stop-thriftserver.sh
sleep 15
/usr/local/hadoop/spark-release/sbin/start-thriftserver.sh --deploy-mode client --hiveconf hive.server2.thrift.port=14000 --hiveconf hive.server2.authentication.kerberos.keytab=/etc/security/keytab/eos.keytab --hiveconf hive.server2.authentication.kerberos.principal=eos@ENIOT.IO
sleep 80

cd /usr/local/hadoop/spark-release/bin
beeline -u "jdbc:hive2://localhost:14000/wall"
