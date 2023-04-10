# One-Class SVM流式异常检测 (OcsvmOutlierStreamOp)
Java 类名：com.alibaba.alink.operator.stream.outlier.OcsvmOutlierStreamOp

Python 类名：OcsvmOutlierStreamOp


## 功能介绍
与传统SVM不同的是，one-class SVM是一种非监督的学习算法，经常被用来做异常点检测。在该算法的训练集中只有一类positive（或者negative）的数据，而没有（或存在极少量）另外一类，通常称其为异常点。该算法需要学习（learn）的就是边界（boundary），而不是最大间隔（maximum margin），通过边界对异常点进行预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| coef0 | Kernel函数的相关参数coef0 |  Kernel函数的相关参数，只有在POLY和SIGMOID时起作用。 | Double |  |  | 0.0 |
| degree | 多项式阶数 | 多项式的阶数，默认2 | Integer |  | x >= 1 | 2 |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | x >= 0.0 | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| gamma | Kernel函数的相关参数gamma | Kernel函数的相关参数，只在 RBF, POLY 和 SIGMOID 时起作用. 如果不设置默认取 1/d，d为特征维度. | Double |  |  | -1.0 |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| kernelType | 核函数类型 | 核函数类型，可取为"RBF"，"POLY"，"SIGMOID"，"LINEAR" | String |  | "RBF", "POLY", "SIGMOID", "LINEAR" | "RBF" |
| nu | 异常点比例上界参数nu | 该参数取值范围是(0,1)，该值与支持向量的数目正向相关。 | Double |  |  | 0.01 |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| precedingRows | 数据窗口大小 | 数据窗口大小 | Integer |  |  | null |
| precedingTime | 时间窗口大小 | 时间窗口大小 | String |  |  | null |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| tensorCol | tensor列 | tensor列 | String |  | 所选列类型为 [BOOL_TENSOR, BYTE_TENSOR, DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR, STRING, STRING_TENSOR, TENSOR, UBYTE_TENSOR] | null |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String |  |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
