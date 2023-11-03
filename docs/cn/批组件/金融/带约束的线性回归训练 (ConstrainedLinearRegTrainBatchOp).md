# 带约束的线性回归训练 (ConstrainedLinearRegTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.finance.ConstrainedLinearRegTrainBatchOp

Python 类名：ConstrainedLinearRegTrainBatchOp


## 功能介绍
* 带约束的线性回归是一个回归算法
* 和线性回归组件一样，带约束的线性回归支持稀疏、稠密两种数据格式
* 和线性回归组件一样，带约束的线性回归支持带样本权重的训练

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| constOptimMethod | 优化方法 | 求解优化问题时选择的优化方法 | String |  | "SQP", "Barrier", "LBFGS", "Newton", "ALM" | "SQP" |
| constraint | 约束 | 约束 | String |  |  | "" |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | x >= 0.0 | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | x >= 0.0 | 0.0 |
| l2 | L2 正则化系数 | L2 正则化系数，默认为0。 | Double |  | x >= 0.0 | 0.0 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | x >= 1 | 100 |
| standardization | 是否正则化 | 是否对训练数据做正则化，默认true | Boolean |  |  | true |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  |  | true |




### 约束说明
约束有larger than value, larger than feature, less than value, less than feature, equal to feature, scale to feature这六种形式。

约束由ConstraintBetweenFeatures类控制，写完ConstraintBetweenFeatures实例以后存放于FeatureConstraint中。

约束可以通过constraint参数传入，也可以在linkFrom中通过表传入。但推荐直接通过constraint参数传入。

约束以如下格式传入，下面表示约束的意义为：

第2列的上界为7；第1列下界为3；第1列和第6列相等；第3列是第4列的7倍；第4列小于等于第5列；第5列大于等于第6列。

{"featureConstraint":[],"constraintBetweenFeatures":{"name":"constraintBetweenFeatures","UP":[[2,7.0]],"LO":[[1,3.0]],"=":[1,6],"%":[3,4,7.0],"<":[4,5],">":[5,6]}}

如果想通过feature colName的方式来表示约束，则以如下形式：

以下表示的是f1列下界是0,1.814，f1列大于等于f0列。

{"featureConstraint":[],"constraintBetweenFeatures":{"name":"constraintBetweenFeatures","UP":[],"LO":[["f1",0,1.814]],"=":[],"%":[],"<":[["f0",0,"f1",0]],">":[]}}

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["1.0 7.0 9.0", 16.8],
    ["1.0 3.0 3.0", 6.7],
    ["1.0 2.0 4.0", 6.9],
    ["1.0 3.0 4.0", 8.0]
])

data = BatchOperator.fromDataframe(df, schemaStr="vec string, label double")

constraint = pd.DataFrame([
        ['{"featureConstraint":[],"constraintBetweenFeatures":{"name":"constraintBetweenFeatures","UP":[],"LO":[],"=":[[1,1.814],[2,0.4]],"%":[],"<":[],">":[[1,2]]},"countZero":null,"elseNullSave":null}']
])
constraintData = BatchOperator.fromDataframe(constraint, schemaStr='data string')

batchOp = ConstrainedLinearRegTrainBatchOp()\
    .setWithIntercept(True)\
    .setVectorCol("vec")\
    .setConstOptimMethod("barrier")\
    .setLabelCol("label")

model = batchOp.linkFrom(data, constraintData)

predict = LinearRegPredictBatchOp()\
    .setPredictionCol("pred")
    
predict.linkFrom(model, data).print()
```

### 运行结果


| vec | label | pred |
|-----|-------|------|
| 1.0 7.0 9.0 | 16.8 | 17.15 |
| 1.0 3.0 3.0 | 6.7 | 8.45 |
| 1.0 2.0 4.0 | 6.9 | 4.90 |
| 1.0 3.0 4.0 | 8.0 | 7.90 |

