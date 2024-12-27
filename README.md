
# Desc 
Read RDS snapshot  parquet file , and send to kafka MQ.


```shell
only supprot JDK8 ( hadoop parquet bug )
```

# build

```bash
mvn clean package
```
# start 
edit properties

`vim config.properties`

```properties

#kafka.bootstrap=192.168.2.246:29092,192.168.2.246:39092,192.168.2.246:49092
kafka.bootstrap=192.168.2.246:29092,192.168.2.246:29093,192.168.2.246:2909
kafka.topic=mgs_test
rds.parquet.path=/tmp/download/access_log_2407
rds.parquet.table=access_log
#表字段 与 kafka msg key 映射
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
