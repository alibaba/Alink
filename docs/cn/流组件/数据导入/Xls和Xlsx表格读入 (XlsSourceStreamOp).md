# Xls和Xlsx表格读入 (XlsSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.XlsSourceStreamOp

Python 类名：XlsSourceStreamOp


## 功能介绍
读xls和xlsx后缀名的表格文件，使用时需要下载xls插件。表格文件可能包含的数据类型有string、int、
long、double、float、date、time、datetime、timestamp，来源可以是本地，oss，http，hdfs等。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| ignoreFirstLine | 是否忽略第一行数据 | 是否忽略第一行数据 | Boolean |  |  | false |
| lenient | 是否容错 | 若为true，当解析失败时丢弃该数据；若为false，解析失败是抛异常 | Boolean |  |  | false |
| sheetIndex | 表格的Sheet编号 | 表格的Sheet编号 | Integer |  |  | 0 |

