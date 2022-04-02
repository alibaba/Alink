# 等宽离散化 (EqualWidthDiscretizer)
Java 类名：com.alibaba.alink.pipeline.feature.EqualWidthDiscretizer

Python 类名：EqualWidthDiscretizer


## 功能介绍

等宽离散可以计算选定数值列的分位点，每个区间都有相同的组距，也就是数据范围/组数，通过训练可以得到一系列分为点，
然后使用这些分位点进行预测。
其中可以所有列使用同一个分组数量，也可以每一列对应一个分组数量。预测结果可以是特征值或一系列0/1离散特征。

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
$$ vectorSize = numBuckets - dropLast(true: 1, false: 0) + (handleInvalid: keep(1), skip(0), error(0)) $$


    numBuckets: 训练参数

    dropLast: 预测参数

    handleInvalid: 预测参数

#### Token index
##### Encode ——> Vector

    1. 正常数据: 唯一的非零元为数据所在的bucket,若 dropLast为true, 最大的bucket的值会被丢掉，预测结果为全零元

    2. null: 
        2.1 handleInvalid为keep: 唯一的非零元为:numBuckets - dropLast(true: 1, false: 0)
        2.2 handleInvalid为skip: null
        2.3 handleInvalid为error: 报错

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| dropLast | 是否删除最后一个元素 | 删除最后一个元素是为了保证线性无关性。默认true | Boolean |  | true |
| encode | 编码方法 | 编码方法 | String |  | "INDEX" |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP" |
| leftOpen | 是否左开右闭 | 左开右闭为true，左闭右开为false | Boolean |  | true |
| numBuckets | quantile个数 | quantile个数，对所有列有效。 | Integer |  | 2 |
| numBucketsArray | quantile个数 | quantile个数，每一列对应数组中一个元素。 | Integer[] |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["a", 1, 1.1],     
    ["b", -2, 0.9],    
    ["c", 100, -0.01], 
    ["d", -99, 100.9], 
    ["a", 1, 1.1],     
    ["b", -2, 0.9],    
    ["c", 100, -0.01], 
    ["d", -99, 100.9] 
])

batchSource =  BatchOperator.fromDataframe(df,schemaStr="f_string string, f_long long, f_double double")
streamSource = StreamOperator.fromDataframe(df,schemaStr="f_string string, f_long long, f_double double")

EqualWidthDiscretizer().setSelectedCols(['f_long', 'f_double']).setNumBuckets(5).fit(batchSource).transform(batchSource).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.feature.EqualWidthDiscretizer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EqualWidthDiscretizerTest {
	@Test
	public void testEqualWidthDiscretizer() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 1, 1.1),
			Row.of("b", -2, 0.9),
			Row.of("c", 100, -0.01),
			Row.of("d", -99, 100.9),
			Row.of("a", 1, 1.1),
			Row.of("b", -2, 0.9),
			Row.of("c", 100, -0.01),
			Row.of("d", -99, 100.9)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(df, "f_string string, f_int int, f_double double");
		new EqualWidthDiscretizer().setSelectedCols("f_int", "f_double").setNumBuckets(5).fit(batchSource).transform(
			batchSource).print();
	}
}
```

### 运行结果
f_string|f_int|f_double
--------|-----|--------
a|2|0
b|2|0
c|4|0
d|0|4
a|2|0
b|2|0
c|4|0
d|0|4
