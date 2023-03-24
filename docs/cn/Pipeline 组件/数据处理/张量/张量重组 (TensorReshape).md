# 张量重组 (TensorReshape)
Java 类名：com.alibaba.alink.pipeline.dataproc.tensor.TensorReshape

Python 类名：TensorReshape


## 功能介绍
对于给定的tensor，按照形状shape返回一个张量，它与原始tensor具有相同的值。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| size | Tensor大小 | Tensor大小 | Integer[] | ✓ |  |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
