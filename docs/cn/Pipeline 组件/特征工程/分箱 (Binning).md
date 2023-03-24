# 分箱 (Binning)
Java 类名：com.alibaba.alink.pipeline.feature.Binning

Python 类名：Binning


## 功能介绍

使用等频，等宽或自动分箱的方法对数据进行分箱操作，输入为连续或离散的特征数据，输出为每个特征的分箱规则，在分箱组件中点键右键选择**我要分箱**，可以对分箱结果进行查看和编辑。

### 算法简介
信用评分卡模型在国外是一种成熟的预测方法，尤其在信用风险评估以及金融风险控制领域更是得到了比较广泛的使用，其原理是将模型变量WOE编码方式离散化之后运用logistic回归模型进行建模，其中本文介绍的分箱算法就是这里说的WOE编码过程。

分箱通常是指对连续变量进行区间划分，将连续变量划分成几个区间变量，主要目的是为了避免“过拟合”，使评分结果更具有稳健性和预测性。这里有个典型的例子就是：用决策树进行评分看起来效果不错，但往往都会因为”过拟合”，模型无法实际应用。

分箱支持等频、等宽两种方法，输入为连续或离散的特征数据，输出为每个特征的分箱规则，分箱结果支持合并、拆分等。

### 算法原理
#### 分箱方法
##### 等频分箱 
等频分箱是对特征数据进行排序，按分位点的方式选取用户指定的N个分位点作为分箱边界，若相邻分位点相同则将两个分箱合并，因此分箱结果中有可能少于用户指定的分箱个数。

##### 等宽分箱 
等宽分箱是对特征数据按最大值和最小值等平均分成N份，在每一等份的边界作为分箱的边界。

##### 离散变量分箱 
字符串类型变量即为离散变量，离散变量的分箱不使用上述的三种分箱方法，对于离散变量的每一种取值会单独分成一个分箱，仅当某个取值小于用户指定的最小阈值时，该取值会被统一归入ELSE分箱中。

#### 相关公式
WOE的计算公式如下:
$$ WOE_i=ln(\dfrac{py_i}{pn_i}) $$

其中，py_i是这个组中正样本个数占所有正样本个数的比例，pn_i是这个组中负样本个数占所有负样本个数的比例。

IV的计算公式如下：
$$ IV_i=(py_i-pn_i)*WOE_i $$


实际使用时，一般用IV评估变量的重要性，IV越大，变量对模型的影响越大，在单个变量内部，再利用WOE判断每个bin的区分度如何，WOE越大，区分度越大。
    
**分箱个数**针对连续变量而言，支持三种类型输入:
    
    i. 输入数值如5，这个数值会对每一列起作用，每列数值型都会被分成5组。

    ii. 输入数组如5,6,7; 这个数组与选择列中数值列的长度必须保持一致，这种输入与第一种/第三种输入是互斥的。

    iii. 输入key-value字符串如age:11,pay:7,bill:11，其中key和value用":"分割，不同特征用","分隔，这种输入与第一种输入一同起作用，用key-value定义的变量会采用key-value中的值，其余变量采用第一种输入中的数值。
    
**离散值个数阈值**针对离散变量而言，指数据中出现次数低于这个值的组会被分到ELSE箱子中，同 **分箱个数** 的输入方法一致。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| binningMethod | 连续特征分箱方法 | 连续特征分箱方法 | String |  | "QUANTILE", "BUCKET" | "QUANTILE" |
| defaultWoe | 默认Woe，在woe为Nan或NULL时替换 | 默认Woe，在woe为Nan或NULL时替换 | Double |  |  | NaN |
| discreteThresholds | 离散个数阈值 | 离散个数阈值，低于该阈值的离散样本将不会单独成一个组别。 | Integer |  |  | -2147483648 |
| discreteThresholdsArray | 离散个数阈值数组 | 离散个数阈值，每一列对应数组中一个元素。 | Integer[] |  |  | null |
| discreteThresholdsMap | 离散分箱离散为ELSE的最小阈值, 形式如col0:3, col1:4 | 离散分箱离散为ELSE的最小阈值，形式如col0:3, col1:4。 | String |  |  | null |
| dropLast | 是否删除最后一个元素 | 删除最后一个元素是为了保证线性无关性。默认true | Boolean |  |  | true |
| encode | 编码方法 | 编码方法 | String |  | "WOE", "VECTOR", "ASSEMBLED_VECTOR", "INDEX" | "ASSEMBLED_VECTOR" |
| fromUserDefined | 是否读取用户自定义JSON | 是否读取用户自定义JSON, true则为用户自定义分箱，false则按参数配置分箱。 | Boolean |  |  | false |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP", "ERROR", "SKIP" | "KEEP" |
| labelCol | 标签列名 | 输入表中的标签列名 | String |  |  | null |
| leftOpen | 是否左开右闭 | 左开右闭为true，左闭右开为false | Boolean |  |  | true |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| numBuckets | quantile个数 | quantile个数，对所有列有效。 | Integer |  |  | 2 |
| numBucketsArray | quantile个数 | quantile个数，每一列对应数组中一个元素。 | Integer[] |  |  | null |
| numBucketsMap | 用户定义的bucket个数，形式如col0:3, col1:4 | 用户定义的bucket个数，形式如col0:3, col1:4 | String |  |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| positiveLabelValueString | 正样本 | 正样本对应的字符串格式。 | String |  |  | "1" |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| userDefinedBin | 用户定义的bin的json | 用户定义的bin的json | String |  |  |  |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.0, True, 0, "A", 1],
    [2.1, False, 2, "B", 1],
    [1.1, True, 3, "C", 1],
    [2.2, True, 1, "E", 0],
    [0.1, True, 2, "A", 0],
    [1.5, False, -4, "D", 1],
    [1.3, True, 1, "B", 0],
    [0.2, True, -1, "A", 1],
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 boolean, f2 int, f3 string, label int')

binning = Binning().setEncode("INDEX").setSelectedCols(["f0", "f1", "f2", "f3"]).setLabelCol("label").setPositiveLabelValueString("1")
binning.fit(inOp1).transform(inOp1).print()
```
 
### 运行结果
```
   f0  f1  f2  f3  label
0   0   1   0   3      1
1   1   0   1   0      1
2   0   1   1   1      1
3   1   1   0   2      0
4   0   1   1   3      0
5   1   0   0   4      1
6   0   1   0   0      0
7   0   1   0   3      1
```
