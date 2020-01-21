## Description
Stream source that reads text lines.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path | String | ✓ |  |
| ignoreFirstLine | Whether to ignore first line of csv file. | Boolean |  | false |
| textCol | Text Column Name | String |  | "text" |


## Script Example
#### Code
```python
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv"
data = TextSourceStreamOp().setFilePath(URL).setTextCol("text")
data.print()
StreamOperator.execute()
```
