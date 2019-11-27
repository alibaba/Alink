## Description
Stream source that reads data from MySql.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| inputTableName | input table name | String | ✓ |  |
| dbName | db name | String | ✓ |  |
| ip | ip | String | ✓ |  |
| password | password | String | ✓ |  |
| port | port | String | ✓ |  |
| username | username | String | ✓ |  |


## Script Example

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


