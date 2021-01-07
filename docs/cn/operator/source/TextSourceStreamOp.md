## 功能介绍

按行读取文件数据

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| ignoreFirstLine | 是否忽略第一行数据 | 是否忽略第一行数据 | Boolean |  | false |
| textCol | 文本列名称 | 文本列名称 | String |  | "text" |

## 脚本示例
#### 脚本代码
```python
URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
data = TextSourceStreamOp().setFilePath(URL).setTextCol("text")
data.print()
StreamOperator.execute()
```
