
# Desc 
Read RDS snapshot  parquet file , and send to kafka MQ.
such as this format:

```
part-00002-b11dc85b-9e64-4fe5-9d04-04840583ad8a-c000.gz.parquet
```

# build

```bash
# only support jdk1.8 ( hadoop parquet bug )

mvn clean package
```
# start 
edit properties

`vim config.properties`

```properties

kafka.bootstrap=192.168.2.246:9093,192.168.2.246:9094,192.168.2.246:9095
kafka.topic=mgs_test
rds.parquet.path=/tmp/download/access_log_2407
rds.parquet.table=access_log

# rds table filed as topic msg key
rds-table-filed-map-as-topic-msg-key=id

```
# start application 

```shell
# only support java version 1.8 

java -Dconfig.file.path=./config.properties \
 -Dlog4j2.level=INFO \
-jar  rds-parquet-to-kafka-mq-tool-1.0.1-SNAPSHOT.jar


# run background 
nohup java  -Xms2g -Xmx4g -XX:+UseG1GC \
 -Dconfig.file.path=./config.properties \
 -Dlog4j2.level=INFO \
  -jar rds-parquet-to-kafka-mq-tool-1.0.1-SNAPSHOT.jar  > out-log.log 2>&1 &

```
