# one-hot编码组件

## 算法介绍

one-hot编码，也称独热编码，对于每一个特征，如果它有m个可能值，那么经过 独热编码后，就变成了m个二元特征。并且，这些特征互斥，每次只有一个激活。 因此，数据会变成稀疏的，输出结果也是kv的稀疏结构。

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 运行脚本
```python
data = np.array([
    ["assisbragasm", 1],
    ["assiseduc", 1],
    ["assist", 1],
    ["assiseduc", 1],
    ["assistebrasil", 1],
    ["assiseduc", 1],
    ["assistebrasil", 1],
    ["assistencialgsamsung", 1]
])

# load data
df = pd.DataFrame({"query": data[:, 0], "weight": data[:, 1]})

inOp = dataframeToOperator(df, schemaStr='query string, weight long', op_type='batch')

# one hot train
one_hot = OneHotTrainBatchOp().setSelectedCols(["query"]).setDropLast(False).setIgnoreNull(False)
model = inOp.link(one_hot)

# batch predict
predictor = OneHotPredictBatchOp().setOutputCol("predicted_r").setReservedCols(["weight"])
print(BatchOperator.collectToDataframe(predictor.linkFrom(model, inOp)))

# stream predict
inOp2 = dataframeToOperator(df, schemaStr='query string, weight long', op_type='stream')
predictor = OneHotPredictStreamOp(model).setOutputCol("predicted_r").setReservedCols(["weight"])
predictor.linkFrom(inOp2).print()

StreamOperator.execute()
```
#### 运行结果

```python
  weight predicted_r
0       1    $6$4:1.0
1       1    $6$3:1.0
2       1    $6$2:1.0
3       1    $6$3:1.0
4       1    $6$1:1.0
5       1    $6$3:1.0
6       1    $6$1:1.0
7       1    $6$0:1.0

```





