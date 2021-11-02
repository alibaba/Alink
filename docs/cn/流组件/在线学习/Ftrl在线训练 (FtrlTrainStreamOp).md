# Ftrl在线训练 (FtrlTrainStreamOp)
Java 类名：com.alibaba.alink.operator.stream.onlinelearning.FtrlTrainStreamOp

Python 类名：FtrlTrainStreamOp


## 功能介绍
该组件是一个在线学习的组件，支持在线实时训练模型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| vectorSize | 向量长度 | 向量的长度 | Integer | ✓ |  |
| alpha | 希腊字母：阿尔法 | 经常用来表示算法特殊的参数 | Double |  | 0.1 |
| beta | 希腊字母：贝塔 | 经常用来表示算法特殊的参数 | Double |  | 1.0 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | 0.0 |
| l2 | 正则化系数 | L2 正则化系数，默认为0。 | Double |  | 0.0 |
| timeInterval | 时间间隔 | 数据流流动过程中时间的间隔 | Integer |  | 1800 |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  | true |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

trainData0 = RandomTableSourceBatchOp() \
            .setNumCols(5) \
            .setNumRows(100) \
            .setOutputCols(["f0", "f1", "f2", "f3", "label"]) \
            .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)")

model = LogisticRegressionTrainBatchOp() \
            .setFeatureCols(["f0", "f1", "f2", "f3"]) \
            .setLabelCol("label") \
            .setMaxIter(10).linkFrom(trainData0)

trainData1 = RandomTableSourceStreamOp() \
            .setNumCols(5) \
            .setMaxRows(10000) \
            .setOutputCols(["f0", "f1", "f2", "f3", "label"]) \
            .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)") \
            .setTimePerSample(0.1)

models = FtrlTrainStreamOp(model) \
            .setFeatureCols(["f0", "f1", "f2", "f3"]) \
            .setLabelCol("label") \
            .setTimeInterval(10) \
            .setAlpha(0.1) \
            .setBeta(0.1) \
            .setL1(0.1) \
            .setL2(0.1)\
            .setVectorSize(4)\
            .setWithIntercept(True) \
            .linkFrom(trainData1).print()
StreamOperator.execute()
```
### Java 代码
```java
package com.alibaba.alink.operator.stream.ml.onlinelearning;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlTrainStreamOp;
import com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp;
import org.junit.Test;

public class FtrlTrainTestTest {
    @Test
    public void FtrlClassification() throws Exception {
        StreamOperator.setParallelism(2);
        BatchOperator trainData0 = new RandomTableSourceBatchOp()
            .setNumCols(5)
            .setNumRows(100L)
            .setOutputCols(new String[]{"f0", "f1", "f2", "f3", "label"})
            .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)");

        BatchOperator model = new LogisticRegressionTrainBatchOp()
            .setFeatureCols(new String[]{"f0", "f1", "f2", "f3"})
            .setLabelCol("label")
            .setMaxIter(10).linkFrom(trainData0);

        StreamOperator trainData1 = new RandomTableSourceStreamOp()
            .setNumCols(5)
            .setMaxRows(100L)
            .setOutputCols(new String[]{"f0", "f1", "f2", "f3", "label"})
            .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)")
            .setTimePerSample(0.1);

        new FtrlTrainStreamOp(model)
            .setFeatureCols(new String[]{"f0", "f1", "f2", "f3"})
            .setLabelCol("label")
            .setTimeInterval(10)
            .setAlpha(0.1)
            .setBeta(0.1)
            .setL1(0.1)
            .setL2(0.1)
            .setVectorSize(4)
            .setWithIntercept(true)
            .linkFrom(trainData1).print();
        StreamOperator.execute();
    }
}
```
### 运行结果
alinkmodelstreamtimestamp|alinkmodelstreamcount|model_id|model_info|label_value
-------------------------|---------------------|--------|----------|-----------
2021-06-10 17:49:50.599|4|0|{"hasInterceptItem":"true","modelName":"\"Logistic Regression\"","labelCol":null,"linearModelType":"\"LR\"","vectorSize":"4"}|null
2021-06-10 17:49:50.599|4|1048576|{"featureColNames":["f0","f1","f2","f3"],"featureColTypes":null,"coefVector":{"data":[0.7535834898120117,0.1413841730068463,0.22745806246300418,0.22262447698184248,0.07959472260346781]},"coefVectors":null,"convergenceInfo":null}|null
2021-06-10 17:49:50.599|4|2251799812636672|null|2.0000
2021-06-10 17:49:50.599|4|2251799812636673|null|1.0000
