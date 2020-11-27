# ftrl模型过滤

## 功能介绍
该组件是对ftrl 实时训练出来的模型进行实时过滤，将指标不好的模型丢弃掉，仅保留达到用户要求的模型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| aucThreshold | 模型过滤的Auc阈值 | 模型过滤的Auc阈值 | Double |  | 0.5 |
| accuracyThreshold | 模型过滤的Accuracy阈值 | 模型过滤的Accuracy阈值 | Double |  | 0.5 |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| positiveLabelValueString | 正样本 | 正样本对应的字符串格式。 | String |  | null |



## 脚本示例
### 脚本代码
```python
from pyalink.alink import *

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
            .setPositiveLabelValueString("1") \
            .setLabelCol("label").linkFrom(models, trainData1).print()

StreamOperator.execute()
```


#### 输出结果

	bid|	ntab|	model_id|	model_infov	|label_value
   ----|--------|-----------|---------------|----------
    1|	4|	0|{"hasInterceptItem":"true","modelName":"\"Logistic Regression\"","labelCol":null,"linearModelType":"\"LR\""}|	None
	1|	4|	1048576|{"featureColNames":["f0","f1","f2","f3"],"featureColTypes":null,"coefVector":{"data":[0.8280681663572065,0.3537777712848352,0.5083041277789442,0.22793741843196036,0.4721781398974956]},"coefVectors":null}|	None
	1|	4|	2.2518e+15|	NaN|	2
	1|	4|	2.2518e+15|	NaN	|1
	2|	4|	0|{"hasInterceptItem":"true","modelName":"\"Logistic Regression\"","labelCol":null,"linearModelType":"\"LR\""}	|None
	2|	4|	1048576	|"featureColNames":["f0","f1","f2","f3"],"featureColTypes":null,"coefVector":{"data":[0.8174541063296323,0.3233180998644922,0.5524446300392163,0.2142597105666327,0.4619937961566057]},"coefVectors":null}|	None
	2|	4|	2.2518e+15|	NaN|	2
	2|	4|	2.2518e+15|	NaN	|1
	3|  4| ... | ... | ...|
