# 字符串近似最近邻模型 (StringApproxNearestNeighborModel)
Java 类名：com.alibaba.alink.pipeline.similarity.StringApproxNearestNeighborModel

Python 类名：StringApproxNearestNeighborModel


## 功能介绍
由 StringApproxNearestNeighbor 组件调用 fit 方法产生，详见 StringApproxNearestNeighbor 组件的文档。


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| radius | radius值 | radius值 | Double |  | null |
| topN | TopN的值 | TopN的值 | Integer |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 代码示例
见 StringApproxNearestNeighbor 组件的文档。
