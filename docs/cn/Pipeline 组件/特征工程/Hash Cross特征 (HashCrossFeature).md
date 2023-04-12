# Hash Cross特征 (HashCrossFeature)
Java 类名：com.alibaba.alink.pipeline.feature.HashCrossFeature

Python 类名：HashCrossFeature


## 功能介绍

将选定的离散列组合成单列的向量类型的数据。

### 算法原理

将选定列的数据的字符串形式以逗号为分隔符拼接起来，然后使用 murmur3_32 函数得到哈希值，并将哈希值通过平移的方式转换至[0, 特征数)之间。

### 使用方式

使用需要设置选取列的列名（selectCols）和输出列名（outputCol），特征数通过参数 numFeatures 设置，特征数也是
输出列中向量的长度。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| numFeatures | 向量维度 | 生成向量长度 | Integer |  |  | 262144 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |

