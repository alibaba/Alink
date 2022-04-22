# 流式模型流输入 (ModelStreamFileSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.ModelStreamFileSourceStreamOp

Python 类名：ModelStreamFileSourceStreamOp


## 功能介绍
从文件系统读模型流。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| scanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String |  |  | null |
| startTime | 起始时间 | 起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |
