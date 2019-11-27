# 多层感知机训练

## 功能介绍
多层感知机多分类模型

## 参数说明

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| layers | 神经网络层大小 | 神经网络层大小 | int[] | ✓ |  |
| blockSize | 数据分块大小，默认值64 | 数据分块大小，默认值64 | Integer |  | 64 |
| initialWeights | 初始权重值 | 初始权重值 | DenseVector |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | 100 |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | 1.0E-6 |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | 0.0 |
| l2 | 正则化系数 | L2 正则化系数，默认为0。 | Double |  | 0.0 |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码
```python
    mlpc = MultilayerPerceptronClassifier() \
        .setVectorCol("bitmap") \
        .setLabelCol("label") \
        .setLayers([628, 100, 100]) \
        .setMaxIter(100) \
        .setPredictionCol("pred_label") \
        .setPredictionDetailCol("pred_detail")

    # mlpc.fit(batch_data.bo_mnist)
    pipeline = Pipeline().add(mlpc)
    model = pipeline.fit(batch_data.bo_mnist)
    model.save('/tmp/mlpc.csv')
    BatchOperator.execute()
```

#### 脚本运行结果

```
-1,"{""schema"":[""model_id BIGINT,model_info VARCHAR,label_value BIGINT""],""param"":[""{\""vectorCol\"":\""\\\""bitmap\\\""\"",\""maxIter\"":\""100\"",\""layers\"":\""[628,100,100]\"",\""labelCol\"":\""\\\""label\\\""\"",\""predictionCol\"":\""\\\""pred_label\\\""\"",\""predictionDetailCol\"":\""\\\""pred_detail\\\""\""}""],""clazz"":[""com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassificationModel""]}"
0,"0^{""vectorCol"":""\""bitmap\"""",""isVectorInput"":""true"",""layers"":""[628,100,100]"",""featureCols"":null}^"
......
```
