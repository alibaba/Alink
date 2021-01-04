# AFT生存回归

## 功能介绍

在生存分析领域，加速失效时间模型(accelerated failure time model,AFT 模型)可以作为比例风险模型的替代模型。AFT模型将线性回归模型的建模方法引人到生存分析的领域， 将生存时间的对数作为反应变量，研究多协变量与对数生存时间之间的回归关系，在形式上，模型与一般的线性回归模型相似。对回归系数的解释也与一般的线性回归模型相似，较之Cox模型， AFT模型对分析结果的解释更加简单、直观且易于理解，并且可以预测个体的生存时间。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| censorCol | 生存列名 | 生存列名 | String | ✓ |  |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | 100 |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | 1.0E-6 |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  | true |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | 0.0 |
| l2 | 正则化系数 | L2 正则化系数，默认为0。 | Double |  | 0.0 |



## 脚本示例
### 脚本代码
```python
data = np.array([[1.218, 1.0, "1.560,-0.605"],\
[2.949, 0.0, "0.346,2.158"],\
[3.627, 0.0, "1.380,0.231"],\
[0.273, 1.0, "0.520,1.151"],\
[4.199, 0.0, "0.795,-0.226"]])
df = pd.DataFrame({"label":data[:,0], "censor":data[:,1],"features":data[:,2]})
data = dataframeToOperator(df, schemaStr="label double, censor double, features string",op_type="batch")

trainOp = AftSurvivalRegTrainBatchOp()\
				.setVectorCol("features")\
					.setLabelCol("label")\
				.setCensorCol("censor")
model = trainOp.linkFrom(data)
predictOp = AftSurvivalRegPredictBatchOp().setPredictionCol("pred")
predictOp.linkFrom(model, data).collectToDataframe()
```

### 脚本运行结果
#### 模型结果

| model_id   | model_info | label_value |
| --- | --- | --- |
| 0          | {"hasInterceptItem":"true","vectorCol":"\"features\"","modelName":"\"AFTSurvivalRegTrainBatchOp\"","labelCol":null,"linearModelType":"\"AFT\"","vectorSize":"3"} | NULL        |
| 1048576    | {"featureColNames":null,"featureColTypes":null,"coefVector":{"data":[2.6373721387804276,-0.49591581739360013,0.19847648151323818,1.5469720551612485]},"coefVectors":null} | NULL        |

#### 预测结果
| label      | censor     | features   | pred       |
| --- | --- | --- | --- |
| 0.273      | 1.0        | 0.520,1.151 | 13.571097451777327 |
| 1.218      | 1.0        | 1.560,-0.605 | 5.718263596902868 |
| 3.627      | 0.0        | 1.380,0.231 | 7.380610641992667 |
| 4.199      | 0.0        | 0.795,-0.226 | 9.009354073821902 |
| 2.949      | 0.0        | 0.346,2.158 | 18.067188679653064 |







