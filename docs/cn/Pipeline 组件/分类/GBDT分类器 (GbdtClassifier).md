# GBDT分类器 (GbdtClassifier)
Java 类名：com.alibaba.alink.pipeline.classification.GbdtClassifier

Python 类名：GbdtClassifier


## 功能介绍

- gbdt(Gradient Boosting Decision Trees)二分类，是经典的基于boosting的有监督学习模型，可以用来解决二分类问题

- 支持连续特征和离散特征

- 支持数据采样和特征采样

- 目标分类必须是两个

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| learningRate | 学习率 | 学习率（默认为0.3） | Double |  | 0.3 |
| minSumHessianPerLeaf | 叶子节点最小Hessian值 | 叶子节点最小Hessian值（默认为0） | Double |  | 0.0 |
| lambda | xgboost中的l1正则项 | xgboost中的l1正则项 | Double |  | 0.0 |
| gamma | xgboost中的l2正则项 | xgboost中的l2正则项 | Double |  | 0.0 |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |
| featureImportanceType | 特征重要性类型 | 特征重要性类型（默认为GAIN） | String |  | "GAIN" |
| featureSubsamplingRatio | 每棵树特征采样的比例 | 每棵树特征采样的比例，范围为(0, 1]。 | Double |  | 1.0 |
| maxBins | 连续特征进行分箱的最大个数 | 连续特征进行分箱的最大个数。 | Integer |  | 128 |
| maxDepth | 树的深度限制 | 树的深度限制 | Integer |  | 6 |
| maxLeaves | 叶节点的最多个数 | 叶节点的最多个数 | Integer |  | 2147483647 |
| minInfoGain | 分裂的最小增益 | 分裂的最小增益 | Double |  | 0.0 |
| minSampleRatioPerChild | 子节点占父节点的最小样本比例 | 子节点占父节点的最小样本比例 | Double |  | 0.0 |
| minSamplesPerLeaf | 叶节点的最小样本个数 | 叶节点的最小样本个数 | Integer |  | 100 |
| newtonStep | 是否使用二阶梯度 | 是否使用二阶梯度 | Boolean |  | true |
| numTrees | 模型中树的棵数 | 模型中树的棵数 | Integer |  | 100 |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| subsamplingRatio | 每棵树的样本采样比例或采样行数 | 每棵树的样本采样比例或采样行数，行数上限100w行 | Double |  | 1.0 |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |

### 参数建议
对于训练效果来说，比较重要的参数是 树的棵树+学习率、叶子节点最小样本数、单颗树最大深度、特征采样比例。

单个离散特征的取值种类数不能超过256，否则会出错。

## 代码示例
```python
df = pd.DataFrame([
    [1.0, "A", 0, 0, 0],
    [2.0, "B", 1, 1, 0],
    [3.0, "C", 2, 2, 1],
    [4.0, "D", 3, 3, 1]
])

batchSource = BatchOperator.fromDataframe(
    df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')
streamSource = StreamOperator.fromDataframe(
    df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')

GbdtClassifier()\
    .setLearningRate(1.0)\
    .setNumTrees(3)\
    .setMinSamplesPerLeaf(1)\
    .setPredictionDetailCol('pred_detail')\
    .setPredictionCol('pred')\
    .setLabelCol('label')\
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
    .fit(batchSource)\
    .transform(batchSource)\
    .print()

GbdtClassifier()\
    .setLearningRate(1.0)\
    .setNumTrees(3)\
    .setMinSamplesPerLeaf(1)\
    .setPredictionDetailCol('pred_detail')\
    .setPredictionCol('pred')\
    .setLabelCol('label')\
    .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
    .fit(batchSource)\
    .transform(streamSource)\
    .print()

StreamOperator.execute()
```
### 运行结果

f0|f1|f2|f3|label|pred|pred_detail
---|---|---|---|-----|----|-----------
1.0000|A|0|0|0|0|{"0":0.9849144946061075,"1":0.01508550539389248}
2.0000|B|1|1|0|0|{"0":0.9849144946061075,"1":0.01508550539389248}
3.0000|C|2|2|1|1|{"0":0.015085505393892529,"1":0.9849144946061075}
4.0000|D|3|3|1|1|{"0":0.015085505393892529,"1":0.9849144946061075}
