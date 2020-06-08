# Hive

## Description
Reading Hive table（Stream）


## Parameters

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- | --- |
| hiveConfDir | Hive conf dir | local or hdfs dir, for example: hdfs://192.168.99.102:9000/hive-2.0.1/conf | String | ✓ |  |
| hiveVersion | hive version | Hive version | String | ✓ |  |
| dbName | hive DB name | hive DB name | String | ✓ |  |
| inputTableName | Name of input table | Name of input table | String | ✓ |  |
| partitions | partition name | for example "ds=2022/dt=01,ds=2022/dt=02" | String |  |  |
<!-- This is the end of auto-generated parameter info -->


## Script Example
```python
data = HiveSourceStreamOp()\
            .setInputTableName("tbl")\
            .setPartitions("ds=2022/dt=01,ds=2022/dt=02")\
            .setHiveVersion("2.0.1")\
            .setHiveConfDir("hdfs://192.168.99.102:9000/hive-2.0.1/conf")\
            .setDbName("mydb")
```

## Dependency management

#### The URL of JARs

We support different Hive version. For each Hive version, we package their dependencies to a JAR.
Users should place the JAR to correct places as described below.

| version | url |
| ---- | ---- |
| 2.0.x | https://alink-release.oss-cn-beijing.aliyuncs.com/hive-deps-files/hive-deps-2-0-v0.1.jar |
| 2.1.x | https://alink-release.oss-cn-beijing.aliyuncs.com/hive-deps-files/hive-deps-2-1-v0.1.jar |
| 2.2.x | https://alink-release.oss-cn-beijing.aliyuncs.com/hive-deps-files/hive-deps-2-2-v0.1.jar |
| 2.3.x | https://alink-release.oss-cn-beijing.aliyuncs.com/hive-deps-files/hive-deps-2-3-v0.1.jar |
| 3.1.x | https://alink-release.oss-cn-beijing.aliyuncs.com/hive-deps-files/hive-deps-3-1-v0.1.jar |


#### python

The Hive jar should be placed to "${PYALINK_DIR}/lib/"


#### java

First, we should add the following dependency to pom. (Tak Alink 1.1.2、Flink 1.10 for example)：
```
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_connectors_hive_flink-1.10_2.11</artifactId>
    <version>1.1.2</version>
</dependency>
```

More over, the Hive jar should be placed to  "${FLINK_HOME}/lib/"