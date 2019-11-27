## 功能介绍

按libsvm格式读文件。若为稀疏格式数据，id会减1，因为alink的稀疏向量的索引是从0开始的。

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 运行脚本
```
data = LibSvmSourceBatchOp().setFilePath('/tmp/data.txt')
```