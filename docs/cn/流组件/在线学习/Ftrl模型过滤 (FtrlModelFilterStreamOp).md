# Ftrl模型过滤 (FtrlModelFilterStreamOp)
Java 类名：com.alibaba.alink.operator.stream.onlinelearning.FtrlModelFilterStreamOp

Python 类名：FtrlModelFilterStreamOp


## 功能介绍
该组件是对ftrl 实时训练出来的模型进行实时过滤，将指标不好的模型丢弃掉，仅保留达到用户要求的模型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| accuracyThreshold | 模型过滤的Accuracy阈值 | 模型过滤的Accuracy阈值 | Double |  |  | 0.5 |
| aucThreshold | 模型过滤的Auc阈值 | 模型过滤的Auc阈值 | Double |  |  | 0.5 |
| positiveLabelValueString | 正样本 | 正样本对应的字符串格式。 | String |  |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |



## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
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

models = FtrlTrainStreamOp(model, None) \
            .setFeatureCols(["f0", "f1", "f2", "f3"]) \
            .setLabelCol("label") \
            .setTimeInterval(10) \
            .setAlpha(0.1) \
            .setBeta(0.1) \
            .setL1(0.1) \
            .setL2(0.1)\
            .setVectorSize(4)\
            .setWithIntercept(True) \
            .linkFrom(trainData1)

FtrlModelFilterStreamOp().setAucThreshold(0.5).setAccuracyThreshold(0.5) \
            .setPositiveLabelValueString("1.0") \
            .setLabelCol("label").linkFrom(models, trainData1).print()

StreamOperator.execute()
```

### Java 代码
```java
package com.alibaba.alink.operator.stream.ml.onlinelearning;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlModelFilterStreamOp;
import com.alibaba.alink.operator.stream.onlinelearning.FtrlPredictStreamOp;
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
            .setMaxRows(1000L)
            .setOutputCols(new String[]{"f0", "f1", "f2", "f3", "label"})
            .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)")
            .setTimePerSample(0.1);

        StreamOperator smodel = new FtrlTrainStreamOp(model)
            .setFeatureCols(new String[]{"f0", "f1", "f2", "f3"})
            .setLabelCol("label")
            .setTimeInterval(10)
            .setAlpha(0.1)
            .setBeta(0.1)
            .setL1(0.1)
            .setL2(0.1)
            .setVectorSize(4)
            .setWithIntercept(true)
            .linkFrom(trainData1);

        new FtrlModelFilterStreamOp().setAucThreshold(0.5).setAccuracyThreshold(0.5)
            .setPositiveLabelValueString("1.0")
            .setLabelCol("label").linkFrom(smodel, trainData1).print();

        StreamOperator.execute();
    }
}
```

#### 输出结果

alinkmodelstreamtimestamp|alinkmodelstreamcount|model_id|model_info|label_value
-------------------------|---------------------|--------|----------|-----------
2021-06-10 19:39:41.169|4|1048576|{"featureColNames":["f0","f1","f2","f3"],"featureColTypes":null,"coefVector":{"data":[0.8344505544432526,0.12691581782275618,0.218543815526658,0.22769775985648064,0.05203808913915911]},"coefVectors":null,"convergenceInfo":null}|null
2021-06-10 19:39:41.169|4|0|{"hasInterceptItem":"true","modelName":"\"Logistic Regression\"","labelCol":null,"linearModelType":"\"LR\"","vectorSize":"4"}|null
2021-06-10 19:39:41.169|4|2251799812636673|null|1.0000
2021-06-10 19:39:41.169|4|2251799812636672|null|2.0000
2021-06-10 19:40:11.319|4|0|{"hasInterceptItem":"true","modelName":"\"Logistic Regression\"","labelCol":null,"linearModelType":"\"LR\"","vectorSize":"4"}|null
2021-06-10 19:40:11.319|4|1048576|{"featureColNames":["f0","f1","f2","f3"],"featureColTypes":null,"coefVector":{"data":[0.9795436401762672,0.2366945713649036,0.3644473752499545,0.2654714469214479,0.23195535286616062]},"coefVectors":null,"convergenceInfo":null}|null
2021-06-10 19:40:11.319|4|2251799812636672|null|2.0000
2021-06-10 19:40:11.319|4|2251799812636673|null|1.0000
