# 模型流导出 (ModelStreamFileSinkStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sink.ModelStreamFileSinkStreamOp

Python 类名：ModelStreamFileSinkStreamOp


## 功能介绍
将模型流输出到对应目录

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| numKeepModel | 保存模型的数目 | 实时写出模型的数目上限 | Integer |  |  | 2147483647 |
