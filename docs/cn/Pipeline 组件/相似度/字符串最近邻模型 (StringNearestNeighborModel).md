# 字符串最近邻模型 (StringNearestNeighborModel)
Java 类名：com.alibaba.alink.pipeline.similarity.StringNearestNeighborModel

Python 类名：StringNearestNeighborModel


## 功能介绍
由 StringNearestNeighbor 组件调用 fit 方法产生，详见 StringNearestNeighbor 组件的文档。


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| radius | radius值 | radius值 | Double |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | [1, +inf) | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |


## 代码示例
见 StringNearestNeighbor 组件的文档。
