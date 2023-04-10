# Cross候选特征筛选模型 (CrossCandidateSelectorModel)
Java 类名：com.alibaba.alink.pipeline.feature.CrossCandidateSelectorModel

Python 类名：CrossCandidateSelectorModel


## 功能介绍
由 CrossCandidateSelector 组件调用 fit 方法产生，详见 CrossCandidateSelector 组件的文档。


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| appendOriginalData | 是否输出原数据 | 是否输出原数据 | Boolean |  |  | true |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputFormat | 输出格式 | 输出格式 | String |  | "Dense", "Sparse", "Word" | "Sparse" |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |


## 代码示例
见 CrossCandidateSelector 组件的文档。
