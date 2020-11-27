

# one-hot编码组件

## 算法介绍

one-hot编码，也称独热编码，对于每一个特征，如果它有m个可能值，那么经过 独热编码后，就变成了m个二元特征。并且，这些特征互斥，每次只有一个激活。 因此，数据会变成稀疏的，输出结果也是kv的稀疏结构。

### 编码结果
##### Encode ——> INDEX
预测结果为单个token的index

##### Encode ——> VECTOR
预测结果为稀疏向量:

    1. dropLast为true,向量中非零元个数为0或者1
    2. dropLast为false,向量中非零元个数必定为1

##### Encode ——> ASSEMBLED_VECTOR
预测结果为稀疏向量,是预测选择列中,各列预测为VECTOR时,按照选择顺序ASSEMBLE的结果。

#### 向量维度
##### Encode ——> Vector
<div align=center><img src="http://latex.codecogs.com/gif.latex?vectorSize = distinct token Number - dropLast(true: 1, false: 0) + enableElse(true: 1, false:0) + (handleInvalid: keep(1), skip(0), error(0))" ></div>

    distinct token Number: 训练集中指定列的去重后的token数目

    dropLast: 预测参数

    enableElse: 训练时若填写discreteThresholds或discreteThresholdsArray则为true，默认为false

    handleInvalid: 预测参数

#### Token index
##### Encode ——> Vector

    1. 训练集中出现过的token: 唯一的非零元为模型中token对应的token_index,若 dropLast为true, token_index最大的值会被丢掉，预测结果为全零元

    2. null: 
        2.1 handleInvalid为keep: 唯一的非零元为:distinct token Number - dropLast(true: 1, false: 0)
        2.2 handleInvalid为skip: null
        2.3 handleInvalid为error: 报错

    3. 训练集中未出现过的token: 
        3.1 enableElse为true
            3.1.1 handleInvalid为keep: 唯一的非零元为:distinct token Number - dropLast(true: 1, false: 0) + 1
            3.1.2 handleInvalid为skip: 唯一的非零元为:distinct token Number - dropLast(true: 1, false: 0)
            3.1.3 handleInvalid为error: 唯一的非零元为:distinct token Number - dropLast(true: 1, false: 0)

        3.2 enableElse为false
            3.2.1 handleInvalid为keep: 唯一的非零元为:distinct token Number - dropLast(true: 1, false: 0)
            3.2.2 handleInvalid为skip: null
            3.2.3 handleInvalid为error: 报错


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| discreteThresholds | 离散个数阈值 | 离散个数阈值，低于该阈值的离散样本将不会单独成一个组别。 | Integer |  | -2147483648 |
| discreteThresholdsArray | 离散个数阈值 | 离散个数阈值，每一列对应数组中一个元素。 | Integer[] |  | null |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP" |
| encode | 编码方法 | 编码方法 | String |  | "ASSEMBLED_VECTOR" |
| dropLast | 是否删除最后一个元素 | 删除最后一个元素是为了保证线性无关性。默认true | Boolean |  | true |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 脚本示例
#### 脚本代码
```python
data = np.array([
    ["a", 1],
    ["b", 1],
    ["c", 1],
    ["e", 2],
    ["a", 2],
    ["b", 1],
    ["c", 2],
    ["d", 2],
    [None, 1]
])

# load data
df = pd.DataFrame({"query": data[:, 0], "label": data[:, 1]})

inOp = dataframeToOperator(df, schemaStr='query string, weight long', op_type='batch')

# one hot train
one_hot = OneHotEncoder().setSelectedCols(["query"]).setOutputCols(["output"])
one_hot.fit(inOp).transform(inOp).print()
```

#### 脚本运行结果
```python
  query  weight    output
0     a       1       $5$
1     b       1  $5$0:1.0
2     c       1  $5$1:1.0
3     e       2  $5$3:1.0
4     a       2       $5$
5     b       1  $5$0:1.0
6     c       2  $5$1:1.0
7     d       2  $5$2:1.0
8   NaN       1  $5$4:1.0
```





