## 功能介绍
提供sql的select语句功能

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |



## 脚本示例
### 脚本代码

```python
URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data = data.link(SelectBatchOp().setClause("category as label"))
```
