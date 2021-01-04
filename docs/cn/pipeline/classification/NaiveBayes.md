# 朴素贝叶斯分类器算法
## 功能介绍

* 朴素贝叶斯分类器是一个多分类算法
* 朴素贝叶斯分类器组件支持稀疏、稠密两种数据格式
* 朴素贝叶斯分类器组件支持带样本权重的训练
* 数据特征列可以是离散的也可以是连续的。对于连续特征，朴素贝叶斯算法将使用高斯模型进行计算。
* bigint，int等类型默认认为是连续特征。如果想将其按照离散特征来处理，则可以将特征列名写在categoricalCols中。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| smoothing | 算法参数 | 光滑因子，默认为0.0 | Double |  | 0.0 |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |
| featureCols | 特征列名 | 特征列名，必选 | String[] | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


## 脚本示例
### 运行脚本
```python
data = np.array([
    [1.0, 1.0, 0.0, 1.0, 1],
    [1.0, 0.0, 1.0, 1.0, 1],
    [1.0, 0.0, 1.0, 1.0, 1],
    [0.0, 1.0, 1.0, 0.0, 0],
    [0.0, 1.0, 1.0, 0.0, 0],
    [0.0, 1.0, 1.0, 0.0, 0],
    [0.0, 1.0, 1.0, 0.0, 0],
    [1.0, 1.0, 1.0, 1.0, 1],
    [0.0, 1.0, 1.0, 0.0, 0]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "f2": data[:, 2],
                   "f3": data[:, 3],
                   "label": data[:, 4]})
df["label"] = df["label"].astype('int')
# train data
batchData = dataframeToOperator(df, schemaStr='f0 double, f1 double, f2 double, f3 double, label int', op_type='batch')

colnames = ["f0","f1","f2", "f3"]
# pipeline model
ns = NaiveBayes().setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
model = ns.fit(batchData)
model.transform(batchData).print()
```
### 运行结果

f0 | f1 | f2 | f3 | label | pred
---|----|----|----|-------|----
1.0|1.0|0.0|1.0|1|1
1.0|0.0|1.0|1.0|1|1
1.0|0.0|1.0|1.0|1|1
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
0.0|1.0|1.0|0.0|0|0
1.0|1.0|1.0|1.0|1|1
0.0|1.0|1.0|0.0|0|0




