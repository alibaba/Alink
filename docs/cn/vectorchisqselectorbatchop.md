# Vector卡方筛选

## 功能介绍

针对vector数据，进行特征筛选

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectorType | 筛选类型 | 筛选类型，包含"NumTopFeatures","percentile", "fpr", "fdr", "fwe"五种。 | String |  | "NumTopFeatures" |
| numTopFeatures | 最大的p-value列个数 | 最大的p-value列个数, 默认值50 | Integer |  | 50 |
| percentile | 筛选的百分比 | 筛选的百分比，默认值0.1 | Double |  | 0.1 |
| fpr | p value的阈值 | p value的阈值，默认值0.05 | Double |  | 0.05 |
| fdr | 发现阈值 | 发现阈值, 默认值0.05 | Double |  | 0.05 |
| fwe | 错误率阈值 | 错误率阈值, 默认值0.05 | Double |  | 0.05 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |





