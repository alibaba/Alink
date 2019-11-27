# Ridge回归算法

## 功能介绍
* Ridge回归是一个回归算法
* Ridge回归组件支持稀疏、稠密两种数据格式
* Ridge回归组件支持带样本权重的训练

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| optimMethod | 优化方法 | 优化问题求解时选择的优化方法 | String |  | null |
| lambda | 希腊字母：lambda | 惩罚因子，必选 | Double | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  | true |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | 100 |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| standardization | 是否正则化 | 是否对训练数据做正则化，默认true | Boolean |  | true |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 运行脚本
```python
data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])
df = pd.DataFrame({"f0": data[:, 0], 
                   "f1": data[:, 1],
                   "label": data[:, 2]})

batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
colnames = ["f0","f1"]
ridge = RidgeRegression().setFeatureCols(colnames).setLambda(0.1).setLabelCol("label").setPredictionCol("pred")
model = ridge.fit(batchData)
model.transform(batchData).print()
```
#### 运行结果
f0 | f1 | label | pred
---|----|-------|-----
 2 |  1     | 1 | 0.830304
   3 |  2    |  1 | 1.377312
   4 |  3    |  2 | 1.924320
   2 |  4    |  1 | 1.159119
   2 |  2    |  1 | 0.939909
   4 |  3    |  2 | 1.924320
   1 |  2    |  1 | 0.502506
   5 |  3    |  3 | 2.361724



