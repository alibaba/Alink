# 等宽离散化训练 (EqualWidthDiscretizerTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp

Python 类名：EqualWidthDiscretizerTrainBatchOp


## 功能介绍

等宽离散可以计算选定数值列的分位点，每个区间都有相同的组距，也就是数据范围/组数，通过训练可以得到一系列分为点，
然后使用这些分位点进行预测。
其中可以所有列使用同一个分组数量，也可以每一列对应一个分组数量。预测结果可以是特征值或一系列0/1离散特征。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| leftOpen | 是否左开右闭 | 左开右闭为true，左闭右开为false | Boolean |  |  | true |
| numBuckets | quantile个数 | quantile个数，对所有列有效。 | Integer |  |  | 2 |
| numBucketsArray | quantile个数 | quantile个数，每一列对应数组中一个元素。 | Integer[] |  |  | null |


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

trainOp = EqualWidthDiscretizerTrainBatchOp(). \
setSelectedCols(['f_long', 'f_double']). \
setNumBuckets(5). \
linkFrom(batchSource)

EqualWidthDiscretizerPredictBatchOp(). \
setSelectedCols(['f_long', 'f_double']). \
linkFrom(trainOp,batchSource). \
print()

trainOp = EqualWidthDiscretizerTrainBatchOp().setSelectedCols(['f_long', 'f_double']). \
setNumBucketsArray([5,3]). \
linkFrom(batchSource)

EqualWidthDiscretizerPredictBatchOp(). \
setSelectedCols(['f_long', 'f_double']). \
linkFrom(trainOp,batchSource). \
print()

EqualWidthDiscretizerPredictBatchOp(). \
setEncode("ASSEMBLED_VECTOR"). \
setSelectedCols(['f_long', 'f_double']). \
setOutputCols(["assVec"]). \
linkFrom(trainOp,batchSource).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerPredictBatchOp;
import com.alibaba.alink.operator.batch.feature.EqualWidthDiscretizerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.EqualWidthDiscretizerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.params.feature.HasEncodeWithoutWoe.Encode;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EqualWidthDiscretizerTrainBatchOpTest {
	@Test
	public void testEqualWidthDiscretizerTrainBatchOp2() throws Exception {
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
		BatchOperator <?> batchSource = new MemSourceBatchOp(df, "f_string string, f_long int, f_double double");

		BatchOperator <?> trainOp = new EqualWidthDiscretizerTrainBatchOp().setSelectedCols("f_long", "f_double")
			.setNumBuckets(5).linkFrom(batchSource);

		new EqualWidthDiscretizerPredictBatchOp().setSelectedCols("f_long","f_double")
			.linkFrom(trainOp, batchSource).print();

		BatchOperator trainOp2 = new EqualWidthDiscretizerTrainBatchOp().setSelectedCols("f_long", "f_double")
			.setNumBucketsArray(5,3).linkFrom(batchSource);

		new EqualWidthDiscretizerPredictBatchOp().setSelectedCols("f_long","f_double")
			.linkFrom(trainOp2,batchSource).print();

		new EqualWidthDiscretizerPredictBatchOp().setSelectedCols("f_long","f_double")
			.setEncode(Encode.ASSEMBLED_VECTOR)
			.setOutputCols("assVec")
			.linkFrom(trainOp2,batchSource).print();
	}
}
```

### 运行结果
f_string|f_long|f_double
--------|------|--------
a|2|0
b|2|0
c|4|0
d|0|4
a|2|0
b|2|0
c|4|0
d|0|4

f_string|f_long|f_double
--------|------|--------
a|2|0
b|2|0
c|4|0
d|0|2
a|2|0
b|2|0
c|4|0
d|0|2

f_string|f_long|f_double|assVec
--------|------|--------|------
a|1|1.1000|$8$2:1.0 5:1.0
b|-2|0.9000|$8$2:1.0 5:1.0
c|100|-0.0100|$8$5:1.0
d|-99|100.9000|$8$0:1.0
a|1|1.1000|$8$2:1.0 5:1.0
b|-2|0.9000|$8$2:1.0 5:1.0
c|100|-0.0100|$8$5:1.0
d|-99|100.9000|$8$0:1.0
