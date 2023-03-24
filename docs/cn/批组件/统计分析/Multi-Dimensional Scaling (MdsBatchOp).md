# Multi-Dimensional Scaling (MdsBatchOp)
Java 类名：com.alibaba.alink.operator.batch.statistics.MdsBatchOp

Python 类名：MdsBatchOp


## 功能介绍

Multi-Dimensional Scaling, MDS, is a dimension reduction techniques for high-dimensional data. MDS reduces (projects or embeds) data into a lower-dimensional, usually 2D, space. The object of MDS is to keep the distances between data items in the original space as much as possible. Therefore, MDS can be used to perceive clusters or outliers.


## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| dim | 维度数目 | 降维后的维度数目 | Integer |  |  | 2 |
| outputColPrefix | 输出列的前缀 | 输出列的前缀 | String |  |  | "coord" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |

