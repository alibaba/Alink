# 在线FM训练 (OnlineFmTrainStreamOp)
Java 类名：com.alibaba.alink.operator.stream.onlinelearning.OnlineFmTrainStreamOp

Python 类名：OnlineFmTrainStreamOp


## 功能介绍
OnlineFm 算法是Ftrl算法的升级，在 Ftrl 算法的基础上考虑了二阶项对模型的影响。算法支持稀疏和稠密两种类型的训练数据。

### 算法原理
FM模型是线性模型的升级，是在线性表达式后面加入了新的交叉项特征及对应的权值，FM模型的表达式如下所示：
![](https://img.alicdn.com/imgextra/i1/O1CN01cmatso24OY6CKEvtF_!!6000000007381-2-tps-829-181.png)

这里我们使用 Ftrl 优化算法求解该模型。算法原理细节可以参考文献[1]，优化算法请参考文献[2]。

### 算法使用
FM算法是推荐领域被验证的效果较好的推荐方案之一，在电商、广告、视频、信息流、游戏的推荐领域有广泛应用。

- 备注 ：该组件训练的时候 FeatureCols 和 VectorCol 是两个互斥参数，只能有一个参数来描述算法的输入特征。

### 文献
[1] S. Rendle, "Factorization Machines," 2010 IEEE International Conference on Data Mining, 2010, pp. 995-1000, doi: 10.1109/ICDM.2010.127.

[2] McMahan, H. Brendan, et al. "Ad click prediction: a view from the trenches." Proceedings of the 19th ACM SIGKDD international conference on Knowledge discovery and data mining. 2013.

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| alpha | 希腊字母：阿尔法 | 经常用来表示算法特殊的参数 | Double |  |  | 0.1 |
| beta | 希腊字母：贝塔 | 经常用来表示算法特殊的参数 | Double |  |  | 1.0 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  |  | null |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0.1。 | Double |  | x >= 0.0 | 0.1 |
| l2 | 正则化系数 | L2 正则化系数，默认为0.1。 | Double |  | x >= 0.0 | 0.1 |
| lambda0 | 常数项正则化系数 | 常数项正则化系数 | Double |  |  | 0.0 |
| lambda1 | 线性项正则化系数 | 线性项正则化系数 | Double |  |  | 0.0 |
| lambda2 | 二次项正则化系数 | 二次项正则化系数 | Double |  |  | 0.0 |
| miniBatchSize | Batch大小 | 表示单次OnlineFM单次迭代更新使用的样本数量，建议是并行度的整数倍. | Integer |  |  | 512 |
| numFactor | 因子数 | 因子数 | Integer |  |  | 10 |
| timeInterval | 时间间隔 | 数据流流动过程中时间的间隔 | Integer |  |  | 1800 |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  |  | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  |  | true |
| withLinearItem | 是否含有线性项 | 是否含有线性项 | Boolean |  |  | true |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |

## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
```python
trainData0 = RandomTableSourceBatchOp() \
            .setNumCols(5) \
            .setNumRows(100) \
            .setOutputCols(["f0", "f1", "f2", "f3", "label"]) \
            .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)")

model = FmClassifierTrainBatchOp() \
            .setFeatureCols(["f0", "f1", "f2", "f3"]) \
            .setNumEpochs(1) \
            .setWithIntercept(true) \
            .setWithLinearItem(true) \
            .setNumFactor(10) \
            .setLabelCol("label").linkFrom(trainData0)

trainData1 = RandomTableSourceStreamOp() \
            .setNumCols(5) \
            .setMaxRows(10000) \
            .setOutputCols(["f0", "f1", "f2", "f3", "label"]) \
            .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)") \
            .setTimePerSample(0.1)

smodel = OnlineFmTrainStreamOp(model)\
            .setFeatureCols(["f0", "f1", "f2", "f3"])\
            .setLabelCol("label")\
            .setTimeInterval(5)\
            .setAlpha(0.1)\
            .setBeta(0.1)\
            .setL1(1.0e-4)\
            .setL2(1.0e-4)\
            .setWithIntercept(true)\
            .setWithLinearItem(true)\
            .setNumFactor(10)\
            .setMiniBatchSize(4)\
            .linkFrom(trainData1)
smodel.print()
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

		BatchOperator model = new FmClassifierTrainBatchOp()
			.setFeatureCols(new String[]{"f0", "f1", "f2", "f3"})
			.setNumEpochs(1)
			.setWithIntercept(true)
			.setWithLinearItem(true)
			.setNumFactor(10)
			.setLabelCol("label").linkFrom(trainData0);

        StreamOperator trainData1 = new RandomTableSourceStreamOp()
            .setNumCols(5)
            .setMaxRows(100L)
            .setOutputCols(new String[]{"f0", "f1", "f2", "f3", "label"})
            .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)")
            .setTimePerSample(0.1);

		StreamOperator smodel = new OnlineFmTrainStreamOp(model)
			.setFeatureCols(new String[]{"f0", "f1", "f2", "f3"})
			.setLabelCol("label")
			.setTimeInterval(5)
			.setAlpha(0.1)
			.setBeta(0.1)
			.setL1(1.0e-4)
			.setL2(1.0e-4)
			.setWithIntercept(true)
			.setWithLinearItem(true)
			.setNumFactor(10)
			.setMiniBatchSize(4)
			.linkFrom(trainData1);
		smodel.print();
        StreamOperator.execute();
    }
}
```
### 运行结果
alinkmodelstreamtimestamp|alinkmodelstreamcount|feature_id|feature_weights|label_type
-------------------------|---------------------|----------|---------------|----------
2022-06-17 11:35:12.286|6|null|{"vectorColName":null,"labelColName":"\"label\"","labelValues":"[2.0,1.0]","task":"\"BINARY_CLASSIFICATION\"","dim":"[1,1,10]","vectorSize":"4","lossCurve":"[0.6307576690104946,0.34130291005290997,0.86]","featureColNames":"[\"f0\",\"f1\",\"f2\",\"f3\"]"}|null
2022-06-17 11:35:12.286|6|0|[0.07401741132183576,0.06648742054692215,-0.07156106277829404,0.08270553675481662,0.03729909036368983,0.1017925835134536,0.06395293845926697,0.002710645111507162,-0.07456289813117634,-0.06074992927650826,0.20282668994956654]|null
2022-06-17 11:35:12.286|6|1|[0.06320664675357679,0.0770259532705278,-0.06979259575978414,0.101544201376189,0.04663010144478446,0.10986248522034242,0.08160172125552437,0.0050190753204412755,-0.0858894553397876,-0.07143724068176713,0.26361807219377287]|null
2022-06-17 11:35:12.286|6|2|[0.06586036786153898,0.0683656950607503,-0.07939539475173335,0.08911018406926323,0.04141014788185629,0.09345993696450056,0.07527535737550994,0.0042593545311886884,-0.08029395731770174,-0.06355289726568324,0.2563977772496345]|null
2022-06-17 11:35:12.286|6|3|[0.07867881602562854,0.07149932133476265,-0.08276899036131362,0.08616652987791219,0.03611638139965758,0.11224704359799328,0.07292359292949942,0.00669055210555388,-0.08089799819972497,-0.06221340818723913,0.23125470059102257]|null
2022-06-17 11:35:12.286|6|-1|[0.2794136617284648]|null
