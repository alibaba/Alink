# 查找近日特征 (LookupRecentDaysBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.LookupRecentDaysBatchOp

Python 类名：LookupRecentDaysBatchOp


## 功能介绍

查询 Latest N Days 特征生成。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| featureSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如 "f0 string, f1 bigint, f2 double" | String |  |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamUpdateMethod | 模型更新方法 | 模型更新方法，可选COMPLETE（全量更新）或者 INCREMENT（增量更新） | String |  | "COMPLETE", "INCREMENT" | "COMPLETE" |

