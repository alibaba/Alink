# 缺失值填充训练 (ImputerTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.ImputerTrainBatchOp

Python 类名：ImputerTrainBatchOp


## 功能介绍

数据缺失值模型训练

缺失值填充支持4种策略，最大值、最小值、均值、指定数值。当策略为指定数值时，需要设置参数fillValue。

模型生成后处理其他数据参考ImputerPredictBatchOp

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| fillValue | 填充缺失值 | 自定义的填充值。当strategy为value时，读取fillValue的值 | String |  |  | null |
| strategy | 缺失值填充规则 | 缺失值填充的规则，支持mean，max，min或者value。选择value时，需要读取fillValue的值 | String |  | "MEAN", "MIN", "MAX", "VALUE" | "MEAN" |




## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
            ["a", 10.0, 100],
            ["b", -2.5, 9],
            ["c", 100.2, 1],
            ["d", -99.9, 100],
            ["a", 1.4, 1],
            ["b", -2.2, 9],
            ["c", 100.9, 1],
            [None, None, None]
])
             
colnames = ["col1", "col2", "col3"]
selectedColNames = ["col2", "col3"]

inOp = BatchOperator.fromDataframe(df_data, schemaStr='col1 string, col2 double, col3 double')

# train
trainOp = ImputerTrainBatchOp()\
           .setSelectedCols(selectedColNames)

model = trainOp.linkFrom(inOp)

# batch predict
predictOp = ImputerPredictBatchOp()
predictOp.linkFrom(model, inOp).print()

# stream predict
sinOp = StreamOperator.fromDataframe(df_data, schemaStr='col1 string, col2 double, col3 double')

predictStreamOp = ImputerPredictStreamOp(model)
predictStreamOp.linkFrom(sinOp).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.ImputerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.ImputerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.ImputerPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ImputerTrainBatchOpTest {
	@Test
	public void testImputerTrainBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("a", 10.0, 100),
			Row.of("b", -2.5, 9),
			Row.of("c", 100.2, 1),
			Row.of("d", -99.9, 100),
			Row.of("a", 1.4, 1),
			Row.of("b", -2.2, 9),
			Row.of("c", 100.9, 1),
			Row.of(null, null, null)
		);

		String[] selectedColNames = new String[] {"col2", "col3"};
		BatchOperator <?> inOp = new MemSourceBatchOp(df_data, "col1 string, col2 double, col3 int");
		BatchOperator <?> trainOp = new ImputerTrainBatchOp()
			.setSelectedCols(selectedColNames);
		BatchOperator model = trainOp.linkFrom(inOp);
		BatchOperator <?> predictOp = new ImputerPredictBatchOp();
		predictOp.linkFrom(model, inOp).print();
		StreamOperator <?> sinOp = new MemSourceStreamOp(df_data, "col1 string, col2 double, col3 int");
		StreamOperator <?> predictStreamOp = new ImputerPredictStreamOp(model);
		predictStreamOp.linkFrom(sinOp).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

| col1  |       col2  | col3  |
|-------|-------------|-------|
|     a |   10.000000 |   100 |
|     b |   -2.500000 |     9 |
|     c |  100.200000 |     1 |
|     d |  -99.900000 |   100 |
|     a |    1.400000 |     1 |
|     b |   -2.200000 |     9 |
|     c |  100.900000 |     1 |
|  null |   15.414286 |    31 |






