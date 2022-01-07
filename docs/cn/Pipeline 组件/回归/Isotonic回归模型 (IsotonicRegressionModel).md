# Isotonic回归模型 (IsotonicRegressionModel)
Java 类名：com.alibaba.alink.pipeline.regression.IsotonicRegressionModel

Python 类名：IsotonicRegressionModel


## 功能介绍
由 IsotonicRegression 组件调用 fit 方法产生，详见 IsotonicRegression 组件的文档。


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |


## 代码示例
见 IsotonicRegression 组件的文档。
