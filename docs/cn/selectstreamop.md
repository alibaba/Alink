## 功能介绍
提供sql的select语句功能

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码

```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data = data.link(SelectStreamOp().setClause("category as label"))
```
