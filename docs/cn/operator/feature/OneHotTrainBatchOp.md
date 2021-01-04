
## 功能介绍

one-hot编码，也称独热编码，对于每一个特征，如果它有m个可能值，那么经过 独热编码后，就变成了m个二元特征。并且，这些特征互斥，每次只有一个激活。 因此，数据会变成稀疏的，输出结果也是kv的稀疏结构。

### 编码结果
#### 输入
| selectedCol0 | selectedCol1 |
| --- | --- |
| a | 1 |
| b | 1 |
| c | 1 |
| d | 2 |
| a | 2 |
| b | 2 |
| c | 2 |
| e | null |
| NULL | 2 |

##### Encode ——> INDEX
预测结果为单个token的index，如0, 1, 2 ...

##### Encode ——> VECTOR
预测结果为稀疏向量:

    1. dropLast为true,向量中非零元个数为0或者1, 如$5, $5$0:1.0或者NULL。
    2. dropLast为false,向量中非零元个数必定为1, 只能是$5$0:1.0或者NULL。

##### Encode ——> ASSEMBLED_VECTOR
预测结果为稀疏向量,是预测选择列中,各列预测为VECTOR时,按照选择顺序ASSEMBLE的结果。

#### 向量维度
##### Encode ——> Vector
<div align=center><img src="http://latex.codecogs.com/gif.latex?vectorSize = distinct token Number - dropLast(true: 1, false: 0) + enableElse(true: 1, false:0) + (handleInvalid: keep(1), skip(0), error(0))" ></div>

    distinct token Number: 训练集中指定列的去重后的token数目

    dropLast: 预测参数

    enableElse: 训练时若填写discreteThresholds或discreteThresholdsArray则为true，默认为false

    handleInvalid: 预测参数

###### 举例
输入列为selectedCol0
```
1. 如果没有填写discreteThresholds，那么enableElse为false，distinct token Number为(a,b,c,d,e)一共5个token
    1.1 dropLast为True
        1.1.1 handleInvalid为keep: vectorSize=(5 - 1 + 0 + 1 = 5)
        1.1.2 handleInvalid为skip: vectorSize=(5 - 1 + 0 + 0 = 4)
        1.1.3 handleInvalid为error: vectorSize=(5 - 1 + 0 + 0 = 4)
    1.2 dropLast为False
        1.1.1 handleInvalid为keep: vectorSize=(5 - 0 + 0 + 1 = 6)
        1.2.2 handleInvalid为skip: vectorSize=(5 - 0 + 0 + 0 = 5)
        1.2.3 handleInvalid为error: vectorSize=(5 - 0 + 0 + 0 = 5)
2. 如果discreteThresholds为2, 那么enableElse为true, distinct token Number为(a,b,c)一共3个token
    2.1 dropLast为True
        1.1.1 handleInvalid为keep: vectorSize=(3 - 1 + 1 + 1 = 4)
        1.1.2 handleInvalid为skip: vectorSize=(3 - 1 + 1 + 0 = 3)
        1.1.3 handleInvalid为error: vectorSize=(3 - 1 + 1 + 0 = 3)
    2.2 dropLast为False
        1.1.1 handleInvalid为keep: vectorSize=(3 - 0 + 1 + 1 = 5)
        1.2.2 handleInvalid为skip: vectorSize=(3 - 0 + 1 + 0 = 4)
        1.2.3 handleInvalid为error: vectorSize=(3 - 0 + 1 + 0 = 4)
``` 
 
#### Token index
##### Encode ——> Vector

    1. 训练集中出现过的token: 预测值为模型中token对应的token_index,若 dropLast为true, token_index最大的值会被丢掉，预测结果为全零元

    2. null: 
        2.1 handleInvalid为keep: 预测值为distinct token Number - dropLast(true: 1, false: 0)
        2.2 handleInvalid为skip: null
        2.3 handleInvalid为error: 报错

    3. 训练集中未出现过的token: 
        3.1 enableElse为true
            3.1.1 handleInvalid为keep: 预测值为:distinct token Number - dropLast(true: 1, false: 0) + 1
            3.1.2 handleInvalid为skip: 预测值为:distinct token Number - dropLast(true: 1, false: 0)
            3.1.3 handleInvalid为error: 预测值为:distinct token Number - dropLast(true: 1, false: 0)

        3.2 enableElse为false
            3.2.1 handleInvalid为keep: 预测值为:distinct token Number - dropLast(true: 1, false: 0)
            3.2.2 handleInvalid为skip: null
            3.2.3 handleInvalid为error: 报错

###### 举例
输入列为selectedCol0
1. 如果没有填写discreteThresholds

    假设模型中a,b,c,d,e对应的token index为0,1,2,3,4
    
    1.1 dropLast为True
    
    1.1.1 handleInvalid为keep
    
    | selectedCol0 | Encode为INDEX的输出 | Encode为VECTOR的输出 |
    | --- | --- | --- |
    | a | 0 | $5$0:1.0 |
    | b | 1 | $5$1:1.0 |
    | c | 2 | $5$2:1.0 |
    | d | 3 | $5$3:1.0 |
    | e | 4 (Encode为IDNEX时,dropLast不起作用) | $5$ (最大的token index被drop了) |
    | NULL | 5 | $5$4:1.0 |
           
    1.1.2 handleInvalid为skip: vectorSize=(5 - 1 + 0 + 0 = 4)

    | selectedCol0 | Encode为INDEX的输出 | Encode为VECTOR的输出 |
    | --- | --- | --- |
    | a | 0 | $4$0:1.0 |
    | b | 1 | $4$1:1.0 |
    | c | 2 | $4$2:1.0 |
    | d | 3 | $4$3:1.0 |
    | e | 4 (Encode为IDNEX时,dropLast不起作用) | $4$ (最大的token index被drop了) |
    | NULL | NULL | NULL |   
     
1.1.3 handleInvalid为error: 直接报错
       
    1.2 dropLast为False
    1.1.1 handleInvalid为keep

| selectedCol0 | Encode为INDEX的输出 | Encode为VECTOR的输出 |
| --- | --- |  --- |
| a | 0 | $6$0:1.0 |
| b | 1 | $6$1:1.0 |
| c | 2 | $6$2:1.0 |
| d | 3 | $6$3:1.0 |
| e | 4 (Encode为IDNEX时,dropLast不起作用) | $6$4:1.0 |
| NULL | 5 | $6$5:1.0 |
        
1.2.2 handleInvalid为skip

| selectedCol0 | Encode为INDEX的输出 | Encode为VECTOR的输出 |
| --- | --- | --- |
| a | 0 | $5$0:1.0 |
| b | 1 | $5$1:1.0 |
| c | 2 | $5$2:1.0 |
| d | 3 | $5$3:1.0 |
| e | 4 (Encode为IDNEX时,dropLast不起作用) | $5$4:1.0 |
| NULL | NULL | NULL |
        
    1.2.3 handleInvalid为error: 直接报错
        
2. 如果discreteThresholds为2
    假设模型中a,b,c对应的token index为0,1,2
    2.1 dropLast为True
    1.1.1 handleInvalid为keep: 
        
    | selectedCol0 | Encode为INDEX的输出 | Encode为VECTOR的输出 |
    | --- | --- | --- |
    | a | 0 | $4$0:1.0 |
    | b | 1 | $4$1:1.0 |
    | c | 2 | $4$ (最大的token index被drop了) |
    | d | 4 | $4$3:1.0 (unknown token) |
    | e | 4 | $4$3:1.0 (unknown token) |
    | NULL | 3 | $4$2:1.0 |
             
    1.1.2 handleInvalid为skip: 
    
    | selectedCol0 | Encode为INDEX的输出 | Encode为VECTOR的输出 |
    | --- | --- | --- |
    | a | 0 | $3$0:1.0 |
    | b | 1 | $3$1:1.0 |
    | c | 2 | $3$ (最大的token index被drop了) |
    | d | 3 | $3$2:1.0 (unknown token) |
    | e | 3 | $4$2:1.0 (unknown token) |
    | NULL | NULL | NULL |
        
        1.1.3 handleInvalid为error: 直接报错
        
    2.2 dropLast为False
    1.1.1 handleInvalid为keep:
        
     | selectedCol0 | Encode为INDEX的输出 | Encode为VECTOR的输出 |
     | --- | --- | --- |
     | a | 0 | $5$0:1.0 |
     | b | 1 | $5$1:1.0 |
     | c | 2 | $5$2:1.0 |
     | d | 4 | $5$4:1.0 (unknown token) |
     | e | 4 | $5$4:1.0 (unknown token) |
     | NULL | 3 | $5$3:1.0 |
        
    1.2.2 handleInvalid为skip:
    
     | selectedCol0 | Encode为INDEX的输出 | Encode为VECTOR的输出 |
     | --- | --- |  --- |
     | a | 0 | $4$0:1.0 |
     | b | 1 | $4$1:1.0 |
     | c | 2 | $4$2:1.0 |
     | d | 3 | $4$3:1.0 (unknown token) |
     | e | 3 | $4$3:1.0 (unknown token) |
     | NULL | NULL | NULL |
        
    1.2.3 handleInvalid为error: 直接报错


## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| discreteThresholds | 离散个数阈值 | 离散个数阈值，低于该阈值的离散样本将不会单独成一个组别。 | Integer |  | -2147483648 |
| discreteThresholdsArray | 离散个数阈值 | 离散个数阈值，每一列对应数组中一个元素。 | Integer[] |  | null |


## 脚本示例
#### 脚本代码
```python
import numpy as np
import pandas as pd
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
one_hot = OneHotTrainBatchOp().setSelectedCols(["query"])
model = inOp.link(one_hot)
model.print()

# batch predict
predictor = OneHotPredictBatchOp().setOutputCols(["output"])
print(BatchOperator.collectToDataframe(predictor.linkFrom(model, inOp)))
```

#### 脚本运行结果
##### 模型
```python
   column_index                                              token  \
0            -1  {"selectedCols":"[\"query\"]","selectedColType...   
1             0                                                  b   
2             0                                                  c   
3             0                                                  d   
4             0                                                  e   
5             0                                                  a   

   token_index  
0          NaN  
1            0  
2            1  
3            2  
4            3  
5            4
```

##### 预测
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
