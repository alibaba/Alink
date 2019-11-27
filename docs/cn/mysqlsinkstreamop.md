## 功能介绍
写Mysql表

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputTableName | 输出表名字 | 输出表名字 | String | ✓ |  |
| dbName | 数据库名字 | 数据库名字 | String | ✓ |  |
| ip | IP地址 | IP地址 | String | ✓ |  |
| password | 密码 | 密码 | String | ✓ |  |
| port | 端口 | 端口 | String | ✓ |  |
| username | 用户名 | 用户名 | String | ✓ |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例

#### mysql batch source 

```python
data = MySqlSourceBatchOp()\
     .setDbName("alink")\
     .setUsername("alink")\
     .setPassword(passwd)\
     .setIp("***")\
     .setPort("20001")\
     .setInputTableName("test")
```

#### mysql stream source 

```python
data = MySqlSourceStreamOp()\
     .setDbName("alink")\
     .setUsername("alink")\
     .setPassword(passwd)\
     .setIp("***")\
     .setPort("20001")\
     .setInputTableName("test")
```

####  mysql batch sink

```python
data = MySqlSourceBatchOp()\
     .setDbName("alink")\
     .setUsername("alink")\
     .setPassword(passwd)\
     .setIp("***")\
     .setPort("20001")\
     .setOutputTableName("test_out")
```

####  mysql stream sink

```python
data = MySqlSourceStreamOp()\
     .setDbName("alink")\
     .setUsername("alink")\
     .setPassword(passwd)\
     .setIp("***")\
     .setPort("20001")\
     .setOutputTableName("test_out")
```

