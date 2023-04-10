# Auto Cross (AutoCross)
Java 类名：com.alibaba.alink.pipeline.feature.AutoCross

Python 类名：AutoCross


## 功能介绍
参考论文："AutoCross: Automatic Feature Crossing for Tabular Data in Real-World Applications"

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| binningMethod | 连续特征分箱方法 | 连续特征分箱方法 | String |  | "QUANTILE", "BUCKET" | "QUANTILE" |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |  |
| discreteThresholds | 离散个数阈值 | 离散个数阈值，低于该阈值的离散样本将不会单独成一个组别。 | Integer |  |  | -2147483648 |
| discreteThresholdsArray | 离散个数阈值数组 | 离散个数阈值，每一列对应数组中一个元素。 | Integer[] |  |  | null |
| fixCoefs | 固定的模型参数 | 固定的模型参数 | Boolean |  |  | false |
| fraction | 采样比例 | 采样比例 | Double |  | 0.0 <= x <= 1.0 | 0.8 |
| kCross | k折 | k折 | Integer |  | x >= 1 | 1 |
| maxSearchStep | 特征组合搜索步数 | 特征组合搜索步数 | Integer |  |  | 2 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numBuckets | quantile个数 | quantile个数，对所有列有效。 | Integer |  |  | 2 |
| numBucketsArray | quantile个数 | quantile个数，每一列对应数组中一个元素。 | Integer[] |  |  | null |
| outputFormat | 输出格式 | 输出格式 | String |  | "Dense", "Sparse", "Word" | "Sparse" |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

