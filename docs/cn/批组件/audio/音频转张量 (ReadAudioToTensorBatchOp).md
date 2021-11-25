# 音频转张量 (ReadAudioToTensorBatchOp)
Java 类名：com.alibaba.alink.operator.batch.audio.ReadAudioToTensorBatchOp

Python 类名：ReadAudioToTensorBatchOp


## 功能介绍

读取音频文件，并转换为 Alink Tensor 格式。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| relativeFilePathCol | Not available! | Not available! | String | ✓ |  |
| rootFilePath | 文件路径 | 文件路径 | String | ✓ |  |
| sampleRate | 采样率 | 采样率 | Integer | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
