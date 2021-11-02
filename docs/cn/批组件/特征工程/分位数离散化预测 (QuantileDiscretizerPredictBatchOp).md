# 分位数离散化预测 (QuantileDiscretizerPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.QuantileDiscretizerPredictBatchOp

Python 类名：QuantileDiscretizerPredictBatchOp


## 功能介绍

分位点离散可以计算选定列的分位点，然后使用这些分位点进行离散化。
生成选中列对应的q-quantile，其中可以所有列指定一个，也可以每一列对应一个

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
    ["a", 1, 1, 2.0, True],
    ["c", 1, 2, -3.0, True],
    ["a", 2, 2, 2.0, False],
    ["c", 0, 0, 0.0, False]
])

batchSource = BatchOperator.fromDataframe(
    df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')
streamSource = StreamOperator.fromDataframe(
    df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')

trainOp = QuantileDiscretizerTrainBatchOp()\
    .setSelectedCols(['f_double'])\
    .setNumBuckets(8)\
    .linkFrom(batchSource)
predictBatchOp = QuantileDiscretizerPredictBatchOp()\
    .setSelectedCols(['f_double'])
predictStreamOp = QuantileDiscretizerPredictStreamOp(trainOp)\
    .setSelectedCols(['f_double'])

predictBatchOp.linkFrom(trainOp, batchSource).print()
predictStreamOp.linkFrom(streamSource) .print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.QuantileDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.QuantileDiscretizerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class QuantileDiscretizerPredictBatchOpTest {
	@Test
	public void testQuantileDiscretizerPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 1, 1, 2.0, true),
			Row.of("c", 1, 2, -3.0, true),
			Row.of("a", 2, 2, 2.0, false),
			Row.of("c", 0, 0, 0.0, false)
		);
		BatchOperator <?> batchSource = new MemSourceBatchOp(df,
			"f_string string, f_long int, f_int int, f_double double, f_boolean boolean");
		StreamOperator <?> streamSource = new MemSourceStreamOp(df,
			"f_string string, f_long int, f_int int, f_double double, f_boolean boolean");
		BatchOperator <?> trainOp = new QuantileDiscretizerTrainBatchOp()
			.setSelectedCols("f_double")
			.setNumBuckets(8)
			.linkFrom(batchSource);
		BatchOperator <?> predictBatchOp = new QuantileDiscretizerPredictBatchOp()
			.setSelectedCols("f_double");
		predictBatchOp.linkFrom(trainOp, batchSource).print();
		StreamOperator <?> predictStreamOp = new QuantileDiscretizerPredictStreamOp(trainOp)
			.setSelectedCols("f_double");
		predictStreamOp.linkFrom(streamSource).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
f_string|f_long|f_int|f_double|f_boolean
--------|------|-----|--------|---------
a|1|1|2|true
c|1|2|0|true
a|2|2|2|false
c|0|0|1|false
