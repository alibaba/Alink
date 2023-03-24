# 评分卡训练 (ScorecardTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.finance.ScorecardTrainBatchOp

Python 类名：ScorecardTrainBatchOp


## 功能介绍
评分卡是在信用风险评估领域常用的建模工具，本实现的原理是通过分箱输入将原始变量离散化后再使用线性模型（逻辑回归，线性回归等）进行模型训练，其中包括特征工程/分数转换功能等等，同时也支持训练过程中的给变量添加约束条件。

注：若未指定分箱输入，则评分卡训练过程完全等价于一般的逻辑回归/线性回归。

### 特征工程
评分卡区别于普通的线性模型的最大的地方在于，评分卡在使用线性模型进行训练之前会对数据进行一定的特征工程处理，本评分卡中提供了两种特征工程方法，都是需要先经过分箱将特征编码，

    i. ASSEMBLERED_VECTOR，将所有变量根据分箱结果进行编码生成一个统一的向量。
    ii. WOE，即将变量的原始值使用变量落入的分箱所对应的WOE值进行替换。
    iii. NULL, 不进行编码。

注：使用ASSEMBLERED_VECTOR时，每个原始变量的不同组之间可以设置相关的约束，具体参见后续章节。

### 分数转换
评分卡的信用评分等场景中，需要通过线性变换将预测得到的样本的odds转换成分数，通常通过如下的线性变换来进行：

$$ log(odds)=\sum_{i}w_ix_i=a*scaledScore+b $$


用户通过如下三个参数来指定这个线性变换关系：

    scaledValue: 给出一个分数的基准点
    odds: 在给定的分数基准点处的odds值
    pdo: (Point Double Odds) 分数增长多分odds值加倍
如scaledValue=800, odds=50, pdo=25, 则表示指定了上述直线中的两个点：

    log(50) = a * 800 + b * log(50)
    log(100) = a * 825 + b * log(100)

解出a和b，对模型中的分数做线性变换即可得到转换后的变量分。

### 训练过程支持约束
评分卡训练过程支持对变量添加约束，如可指定某个bin所对应的分数为固定值，或两个bin的分数满足一定的比例，再或者bin之间的分数有大小的限制，如设置bin的分数按bin的woe值排序等等，约束的实现依赖于底层的带约束的优化算法，约束可以在分箱的UI中进行设置，设置完成后分箱会生一个json格式的约束条件，并自动传递给后面连接的训练组件。

目前支持如下几种json约束:

    "<": 变量的权重按顺序满足升序的约束
    ">": 变量的权重按顺序满足降序的约束
    "=": 变量的权重等于固定值
    "%": 变量之间的权重符定一定的比例关系
    "UP": 变量的权重约束上限
    "LO": 变量的权重约束下限

json约束以字符串的形式存储在表中，表为单行单列（字符串类型）的表，存储如下的Json字符串：
```json
{
    "<": [
        [
            4,
            3
        ]
    ],
    "%": [
        [
            0,
            1,
            2
        ],
        [
            2,
            0,
            1
        ]
    ],
    "=": [
        [
            1,
            2
        ]
    ],
    "LO": [
        [
            3,
            5
        ]
    ],
    "name": "fc"
}
```

### 内置约束

### 优化算法
在高级选项中可以选择训练过程中使用的优化算法，目前支持下面四种优化算法:

    L-BFGS
    Newton's Method
    Barrier Method
    SQP

其中L-BFGS是一阶的优化算法支持较大规模的特征数据级，牛顿法是经典的二阶算法，收敛速度快，准确度高，但由于要计算二阶的Hessian Matrix，因此不适用于较大特征规模的问题，这两种算法均为无约束的优化算法，当选择这两种优化算法时会自动忽略约束条件。

当训练过程中有约束条件时，可以选择Barrier Method和SQP，这两种算法都是二阶的优化算法，在没有约束条件的情况下完全等价于牛顿法，这两种算法的计算性能和准确性差别不大，我们默认建议选择SQP。

如果用户对优化算法不太了解，建议默认选择”自动选择“，会自动根据用户任务的数据规模和约束情况来选择最合适的优化算法。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| alphaEntry | 筛选阈值 | 筛选阈值 | Double |  |  | 0.05 |
| alphaStay | 移除阈值 | 移除阈值 | Double |  |  | 0.05 |
| constOptimMethod | 优化方法 | 求解优化问题时选择的优化方法 | String |  | "SQP", "Barrier", "LBFGS", "Newton" | "SQP" |
| defaultWoe | 默认Woe，在woe为Nan或NULL时替换 | 默认Woe，在woe为Nan或NULL时替换 | Double |  |  | NaN |
| encode | 编码方法 | 编码方法 | String |  | "WOE", "ASSEMBLED_VECTOR", "NULL" | "ASSEMBLED_VECTOR" |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | [0.0, +inf) | 1.0E-6 |
| forceSelectedCols | 强制选择的列 | 强制选择的列 | String[] |  |  |  |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | [0.0, +inf) | 0.0 |
| l2 | L2 正则化系数 | L2 正则化系数，默认为0。 | Double |  | [0.0, +inf) | 0.0 |
| linearModelType | 优化方法 | 优化方法 | String |  | "LR", "LinearReg" | "LR" |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | [1, +inf) | 100 |
| odds | 分数基准点处的odds值 | 分数基准点处的odds值 | Double |  |  | null |
| pdo | 分数增长pdo，odds加倍 | 分数增长pdo，odds加倍 | Double |  |  | null |
| positiveLabelValueString | 正样本 | 正样本对应的字符串格式。 | String |  |  | "1" |
| scaleInfo | 是否将模型进行分数转换 | 是否将模型进行分数转换 | Boolean |  |  | false |
| scaledValue | 分数基准点 | 分数基准点 | Double |  |  | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| withSelector | 是否逐步回归 | 是否逐步回归 | Boolean |  |  | false |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.0, True, 0, "A", 1],
    [2.1, False, 2, "B", 1],
    [1.1, True, 3, "C", 1],
    [2.2, True, 1, "E", 0],
    [0.1, True, 2, "A", 0],
    [1.5, False, -4, "D", 1],
    [1.3, True, 1, "B", 0],
    [0.2, True, -1, "A", 1],
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 boolean, f2 int, f3 string, label int')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='f0 double, f1 boolean, f2 int, f3 string, label int')

binning = BinningTrainBatchOp()\
    .setSelectedCols(["f0", "f1", "f2", "f3"])\
    .setLabelCol("label")\
    .setPositiveLabelValueString("1")\
    .linkFrom(inOp1)
    
scorecard = ScorecardTrainBatchOp()\
    .setPositiveLabelValueString("0")\
    .setSelectedCols(["f0", "f1", "f2", "f3"])\
    .setLabelCol("label")\
    .linkFrom(inOp1, binning)
    
predict = ScorecardPredictBatchOp()\
    .setPredictionScoreCol("score")\
    .setPredictionDetailCol("detail")\
    .linkFrom(scorecard, inOp1)
    
predict.lazyPrint(10)
scorecard.print()

predict = ScorecardPredictStreamOp(scorecard)\
    .setPredictionDetailCol("detail")\
    .setPredictionScoreCol("score")\
    .linkFrom(inOp2)
    
predict.print()
StreamOperator.execute()
```

### 运行结果
```
    f0     f1  f2 f3  label  \
0  1.0   True   0  A      1   
1  2.1  False   2  B      1   
2  1.1   True   3  C      1   
3  2.2   True   1  E      0   
4  0.1   True   2  A      0   
5  1.5  False  -4  D      1   
6  1.3   True   1  B      0   
7  0.2   True  -1  A      1   

                                              detail      score  
0  {"0":"0.9999968435780205","1":"3.1564219794555... -12.666068  
1  {"0":"0.9999978934455291","1":"2.1065544708598... -13.070455  
2  {"0":"0.9999972659779248","1":"2.7340220751792... -12.809734  
3  {"0":"1.676491567126348E-6","1":"0.99999832350...  13.298806  
4  {"0":"5.571428878359264E-6","1":"0.99999442857...  12.097853  
5                              {"0":"1.0","1":"0.0"} -53.828587  
6  {"0":"3.3860911581307107E-6","1":"0.9999966139...  12.595831  
7  {"0":"0.9999968435780205","1":"3.1564219794555... -12.666068
```

