# 数据集拆分

## 功能介绍
将数据集按比例拆分为两部分

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| fraction | 拆分到左端的数据比例 | 拆分到左端的数据比例 | Double | ✓ |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
spliter = SplitStreamOp().setFraction(0.4)
train_data = spliter
test_data = spliter.getSideOutput(0)
```
