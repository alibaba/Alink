# 音频转张量 (ReadAudioToTensorStreamOp)
Java 类名：com.alibaba.alink.operator.stream.audio.ReadAudioToTensorStreamOp

Python 类名：ReadAudioToTensorStreamOp


## 功能介绍

读取音频文件，并转换为 Alink Tensor 格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| relativeFilePathCol | 文件路径列 | 文件路径列 | String | ✓ |  |
| rootFilePath | 文件路径 | 文件路径 | String | ✓ |  |
| sampleRate | 采样率 | 采样率 | Integer | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| duration | 采样持续时间 | 采样持续时间 | Double |  |  |
| offset | 采样开始时刻 | 采样开始时刻 | Double |  | 0.0 |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
