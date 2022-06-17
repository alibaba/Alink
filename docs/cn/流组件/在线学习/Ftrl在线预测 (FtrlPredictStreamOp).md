# Ftrl在线预测 (FtrlPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.onlinelearning.FtrlPredictStreamOp

Python 类名：FtrlPredictStreamOp


## 功能介绍
实时更新 ftrl 训练得到的模型流，并使用实时的模型对实时的数据进行预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


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

FtrlPredictStreamOp(model) \
        .setPredictionCol("pred") \
        .setReservedCols(["label"]) \
        .setPredictionDetailCol("details") \
        .linkFrom(models, trainData1).print()
StreamOperator.execute()
```
### Java 代码
```java
package com.alibaba.alink.operator.stream.ml.onlinelearning;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.LogisticRegressionTrainBatchOp;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
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

        new FtrlPredictStreamOp(model)
        .setPredictionCol("pred")
        .setReservedCols(new String[]{"label"})
        .setPredictionDetailCol("details")
        .linkFrom(smodel, trainData1).print();
        StreamOperator.execute();
    }
}
```
### 运行结果
label|pred|details
-----|----|-------
2.0000|2.0000|{"2.0":"0.8407811313273308","1.0":"0.1592188686726692"}
2.0000|2.0000|{"2.0":"0.8094960632541983","1.0":"0.19050393674580168"}
2.0000|2.0000|{"2.0":"0.8685396820088952","1.0":"0.1314603179911048"}
2.0000|2.0000|{"2.0":"0.781050184076571","1.0":"0.218949815923429"}
1.0000|2.0000|{"2.0":"0.8347637657816113","1.0":"0.16523623421838873"}
2.0000|2.0000|{"2.0":"0.9211808843291631","1.0":"0.07881911567083688"}
