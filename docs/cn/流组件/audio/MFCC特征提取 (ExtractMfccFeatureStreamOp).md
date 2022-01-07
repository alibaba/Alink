# MFCC特征提取 (ExtractMfccFeatureStreamOp)
Java 类名：com.alibaba.alink.operator.stream.audio.ExtractMfccFeatureStreamOp

Python 类名：ExtractMfccFeatureStreamOp


## 功能介绍

从 Alink Tensor 格式的音频数据中提取 MFCC 特征。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| sampleRate | 采样率 | 采样率 | Integer | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| windowTime | 一个窗口的时间 | 一个窗口的时间 | Double |  | 0.128 |
| hopTime | 相邻窗口时间间隔 | 相邻窗口时间间隔 | Double |  | 0.032 |
| numMfcc | mfcc参数 | mfcc参数 | Integer |  | 128 |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
