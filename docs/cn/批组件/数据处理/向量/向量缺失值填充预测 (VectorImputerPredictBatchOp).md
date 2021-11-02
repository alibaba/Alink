# 向量缺失值填充预测 (VectorImputerPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerPredictBatchOp

Python 类名：VectorImputerPredictBatchOp


## 功能介绍
使用 Vector 缺失值填充模型对Vector数据进行数据填充。


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
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
    ["1:3,2:4,4:7", 1],
    ["1:3,2:NaN", 3],
    ["2:4,4:5", 4]
])

data = BatchOperator.fromDataframe(df, schemaStr="vec string, id bigint")
vecFill = VectorImputerTrainBatchOp().setSelectedCol("vec")
model = data.link(vecFill)
VectorImputerPredictBatchOp().setOutputCol("vec1").linkFrom(model, data).collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerPredictBatchOp;
import com.alibaba.alink.operator.batch.dataproc.vector.VectorImputerTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorImputerPredictBatchOpTest {
	@Test
	public void testVectorImputerPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1:3,2:4,4:7", 1),
			Row.of("1:3,2:NaN", 3),
			Row.of("2:4,4:5", 4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec string, id int");
		BatchOperator <?> vecFill = new VectorImputerTrainBatchOp().setSelectedCol("vec");
		BatchOperator <?> model = data.link(vecFill);
		new VectorImputerPredictBatchOp().setOutputCol("vec1").linkFrom(model, data).print();
	}
}
```
### 运行结果


| vec         | id   | vec1              |
| ----------- | ---- | ----------------- |
| 1:3,2:4,4:7 | 1    | 1:3.0 2:4.0 4:7.0 |
| 1:3,2:NaN   | 3    | 1:3.0 2:4.0       |
| 2:4,4:5     | 4    | 2:4.0 4:5.0       |
