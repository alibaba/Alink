# 多热编码预测 (MultiHotPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.MultiHotPredictBatchOp

Python 类名：MultiHotPredictBatchOp


## 功能介绍

multi-hot编码，也称多热编码，是与独热编码相对应的一种编码方式。该编码对每一个字符串特征列按照指定分隔符进行分割，分割得到的值存在m个可能值，那么经过多热编码后就变成了m个二元特征。对每一字段编码将会把该字段分割后的每一个值映射到唯一的编码。 因此，编码后的数据会变成稀疏数据，输出结果也是kv的稀疏结构。

组件为多热编码的批式预测组件。

### 编码结果
#### 输入
| col_0 | col_1 |
| --- | --- |
| "a b"| "1 2" |
| "b c"| "1 3" |
| "c d" | "1 4"|
| "a d" | "3 2" |
| "d e" | null |
| NULL | "2 3" |

##### Encode ——> VECTOR
预测结果为稀疏向量:

    向量中非零元个数必定为1, 只能是一个稀疏向量$5$0:1.0 4:1.0或者NULL。

##### Encode ——> ASSEMBLED_VECTOR
    预测结果为稀疏向量,是预测选择列中,各列预测为VECTOR时,按照选择顺序ASSEMBLE的结果。

#### 向量维度
##### Encode ——> Vector
$$ vectorSize = distinct token Number + enableElse(true: 1, false:0) + (handleInvalid: keep(1), skip(0), error(0)) $$


    distinct token Number: 训练集中指定列的去重后的token数目

    enableElse: 训练时若填写discreteThresholds或discreteThresholdsArray则为true，默认为false

    handleInvalid: 预测参数

###### 举例
输入列为col_0
```
1. 如果没有填写discreteThresholds，那么enableElse为false，distinct token Number为(a,b,c,d,e)一共5个token
        1.1.1 handleInvalid为keep: vectorSize=(5 + 0 + 1 = 6)
        1.2.2 handleInvalid为skip: vectorSize=(5 + 0 + 0 = 5)
        1.2.3 handleInvalid为error: vectorSize=(5 + 0 + 0 = 5)
2. 如果discreteThresholds为2, 那么enableElse为true, distinct token Number为(a,b,c,d,e)一共5个token
        1.1.1 handleInvalid为keep: vectorSize=(5 + 1 + 1 = 7)
        1.2.2 handleInvalid为skip: vectorSize=(5 + 1 + 0 = 6)
        1.2.3 handleInvalid为error: vectorSize=(5 + 1 + 0 = 6)
``` 
 
#### Token index
##### Encode ——> Vector

    1. 训练集中出现过的token: 预测值为模型中token对应的token_index

    2. 训练集中未出现过的token: 
        3.1 enableElse为true
            3.1.1 handleInvalid为keep: 预测值为:distinct token Number + 1
            3.1.2 handleInvalid为skip: 预测值为:distinct token Number
            3.1.3 handleInvalid为error: 预测值为:distinct token Number

        3.2 enableElse为false
            3.2.1 handleInvalid为keep: 预测值为:distinct token Number
            3.2.2 handleInvalid为skip: 无index
            3.2.3 handleInvalid为error: 报错

###### 举例
输入列为col_0
1. 如果没有填写discreteThresholds, 假设模型中a,b,c,d,e对应的token index为0,1,2,3,4
    
1.1 handleInvalid为keep
        
| col_0 | Encode为VECTOR的输出 |
| --- | ------------------- |
| "a b"|  $6$0:1.0 1:1.0|
| "b c"| $6$1:1.0 2:1.0 |
| "c d" | $6$3:1.0 3:1.0|
| "a d" | $6$0:1.0 3:1.0 |
| "d e" | $6$0:3.0 4:1.0 |
| NULL | NULL |

   
1.2 handleInvalid为skip

| col_0 | Encode为VECTOR的输出 |
| ----- | -------------------- |
| "a b" | $5$0:1.0 1:1.0       |
| "b c" | $5$1:1.0 2:1.0       |
| "c d" | $5$3:1.0 3:1.0       |
| "a d" | $5$0:1.0 3:1.0       |
| "d e" | $5$0:3.0 4:1.0       |
| NULL  | NULL                 |
         
1.3 handleInvalid为error: 直接报错
       

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，必选 | String[] | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ | 所选列类型为 [STRING] |  |
| encode | 编码方法 | 编码方法 | String |  | "VECTOR", "ASSEMBLED_VECTOR" | "ASSEMBLED_VECTOR" |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP", "ERROR", "SKIP" | "KEEP" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

# load data
df = pd.DataFrame([
    ["a b", 1],
    ["b c", 1],
    ["c d", 1],
    ["a d", 2],
    ["d e", 2],
    [None, 1]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='query string, weight long')

# multi hot train
multi_hot = MultiHotTrainBatchOp().setSelectedCols(["query"])
model = inOp.link(multi_hot)
model.print()

# batch predict
predictor = MultiHotPredictBatchOp().setSelectedCols(["query"]).setOutputCols(["output"])
print(BatchOperator.collectToDataframe(predictor.linkFrom(model, inOp)))
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.MultiHotPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.MultiHotTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MultiHotPredictBatchOpTest {
	@Test
	public void testMultiHotPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a b", 1),
			Row.of("b c", 1),
			Row.of("c d", 1),
			Row.of("a d", 2),
			Row.of("d e", 2),
			Row.of(null, 1)
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "query string, weight int");
		BatchOperator <?> multi_hot = new MultiHotTrainBatchOp().setSelectedCols("query");
		BatchOperator <?> model = inOp.link(multi_hot);
		model.print();
		BatchOperator <?> predictor = new MultiHotPredictBatchOp().setSelectedCols("query").setOutputCols("output");
		predictor.linkFrom(model, inOp).print();
	}
}
```

### 运行结果
##### 模型
model_id|model_info
--------|----------
0|{"delimiter":"\" \""}
1048576|["query","a",0,2]
2097152|["query","b",1,2]
3145728|["query","c",2,2]
4194304|["query","d",3,3]
5242880|["query","e",4,1]

#### 结果
query|weight|output
-----|------|------
a b|1|$6$0:1.0 1:1.0
b c|1|$6$1:1.0 2:1.0
c d|1|$6$2:1.0 3:1.0
a d|2|$6$0:1.0 3:1.0
d e|2|$6$3:1.0 4:1.0
null|1|null
