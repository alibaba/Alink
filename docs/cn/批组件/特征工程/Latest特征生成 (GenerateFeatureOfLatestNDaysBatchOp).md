# Latest特征生成 (GenerateFeatureOfLatestNDaysBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.GenerateFeatureOfLatestNDaysBatchOp

Python 类名：GenerateFeatureOfLatestNDaysBatchOp


## 功能介绍

Latest N Days 特征生成。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| baseDate | 截止日期,格式是yyyy-mm-dd | 截止日期,格式是yyyy-mm-dd | String | ✓ |  |  |
| featureDefinitions | 特征定义 | 特征的具体描述 | String | ✓ |  |  |
| timeCol | 时间戳列(TimeStamp) | 时间戳列(TimeStamp) | String | ✓ |  |  |
| extendFeatures | 扩展特征 | 扩展特征 | String |  |  | null |

