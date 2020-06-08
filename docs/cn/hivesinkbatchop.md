# Hive

## 功能介绍
写Hive表（Batch）


## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| hiveConfDir | Hive配置文件路径 | 本地路径或hdfs路径，路径里需包括hive-site.xml文件。示例：hdfs://192.168.99.102:9000/hive-2.0.1/conf | String | ✓ |  |
| hiveVersion | hive版本号 | hive版本号 | String | ✓ |  |
| dbName | hive数据库名字 | hive数据库名字 | String | ✓ |  |
| outputTableName | 输出表名字 | 输出表名字 | String | ✓ |  |
| partition | 分区名 | 例如"ds=2022/dt=01" | String |  |  |
| overwriteSink | 是否覆盖原有数据 | 是否覆盖原有数据 | Boolean |  | False |
<!-- This is the end of auto-generated parameter info -->


## 脚本示例
```python
sink =  HiveSinkBatchOp()\
            .setHiveVersion("2.0.1")\
            .setHiveConfDir("hdfs://192.168.99.102:9000/hive-2.0.1/conf")\
            .setDbName("mydb")\
            .setOutputTableName("tbl_sink")\
            .setOverwriteSink(True)
```

## 依赖管理

#### jar包路径
Hive数据源支持不同的Hive版本，我们把不同版本的Hive的依赖分别打成了jar包，用户可从如下地址下载：

| version | url |
| ---- | ---- |
| 2.0.x | https://alink-release.oss-cn-beijing.aliyuncs.com/hive-deps-files/hive-deps-2-0-v0.1.jar |
| 2.1.x | https://alink-release.oss-cn-beijing.aliyuncs.com/hive-deps-files/hive-deps-2-1-v0.1.jar |
| 2.2.x | https://alink-release.oss-cn-beijing.aliyuncs.com/hive-deps-files/hive-deps-2-2-v0.1.jar |
| 2.3.x | https://alink-release.oss-cn-beijing.aliyuncs.com/hive-deps-files/hive-deps-2-3-v0.1.jar |
| 3.1.x | https://alink-release.oss-cn-beijing.aliyuncs.com/hive-deps-files/hive-deps-3-1-v0.1.jar |


#### python

须把Hive版本对应的jar包拷贝至目录 "${PYALINK_DIR}/lib/"。这里，${PYALINK_DIR}是pyalink的安装路径，通常
位于python/site-packages目录下。


#### java

首先，须在pom文件中加入如下依赖(以Alink 1.1.2版、Flink 1.10版为例)：
```
<dependency>
    <groupId>com.alibaba.alink</groupId>
    <artifactId>alink_connectors_hive_flink-1.10_2.11</artifactId>
    <version>1.1.2</version>
</dependency>
```

此外，须把Hive版本对应的jar包拷贝至目录 "${FLINK_HOME}/lib/"。这里，${FLINK_HOME}是Flink包的路径。