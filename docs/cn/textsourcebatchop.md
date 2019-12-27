## 功能介绍

按行读取文件数据

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| ignoreFirstLine | 是否忽略第一行数据 | 是否忽略第一行数据 | Boolean |  | false |
| textCol | 文本列名称 | 文本列名称 | String |  | "text" |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码
```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
data = TextSourceBatchOp().setFilePath(URL).setTextCol("text")
data.print()
```