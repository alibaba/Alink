## Description
A data source that reads libsvm format data.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path | String | ✓ |  |


## Script Example
#### Script
```
data = LibSvmSourceBatchOp().setFilePath('/tmp/data.txt')
```
