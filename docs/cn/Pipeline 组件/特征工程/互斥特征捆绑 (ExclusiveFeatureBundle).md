# 互斥特征捆绑 (ExclusiveFeatureBundle)
Java 类名：com.alibaba.alink.pipeline.feature.ExclusiveFeatureBundle

Python 类名：ExclusiveFeatureBundle


## 功能介绍

对于稀疏的高维特征，许多特征是互斥的（即，这些特征从不同时取非零值），利用互斥的性质可以将互斥特征捆绑到一个特征中（称为Exclusive Feature Bundle），从而减少特征的数量。



## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| sparseVectorCol | 稀疏向量列名 | 稀疏向量列对应的列名 | String | ✓ |  |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |


