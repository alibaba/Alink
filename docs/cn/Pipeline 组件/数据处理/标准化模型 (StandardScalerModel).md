# 标准化模型 (StandardScalerModel)
Java 类名：com.alibaba.alink.pipeline.dataproc.StandardScalerModel

Python 类名：StandardScalerModel


## 功能介绍

标准化是对数据进行按正态化处理的组件

标准化模型，用于数据的标准化的处理过程

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
            ["a", 10.0, 100],
            ["b", -2.5, 9],
            ["c", 100.2, 1],
            ["d", -99.9, 100],
            ["a", 1.4, 1],
            ["b", -2.2, 9],
            ["c", 100.9, 1]
])
             
colnames = ["col1", "col2", "col3"]
selectedColNames = ["col2", "col3"]

inOp = BatchOperator.fromDataframe(df, schemaStr='col1 string, col2 double, col3 long')

sinOp = StreamOperator.fromDataframe(df, schemaStr='col1 string, col2 double, col3 long')            

model = StandardScaler()\
           .setSelectedCols(selectedColNames)\
           .fit(inOp)

model.transform(inOp).print()

model.transform(sinOp).print()

StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.dataproc.StandardScaler;
import com.alibaba.alink.pipeline.dataproc.StandardScalerModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StandardScalerModelTest {
	@Test
	public void testStandardScalerModel() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 10.0, 100),
			Row.of("b", -2.5, 9),
			Row.of("c", 100.2, 1),
			Row.of("d", -99.9, 100),
			Row.of("a", 1.4, 1),
			Row.of("b", -2.2, 9),
			Row.of("c", 100.9, 1)
		);

		String[] selectedColNames = new String[] {"col2", "col3"};
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "col1 string, col2 double, col3 int");
		StreamOperator <?> sinOp = new MemSourceStreamOp(df, "col1 string, col2 double, col3 int");
		StandardScalerModel model = new StandardScaler()
			.setSelectedCols(selectedColNames)
			.fit(inOp);
		model.transform(inOp).print();
		model.transform(sinOp).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

col1|col2|col3
----|----|----
a|-0.0784|1.4596
b|-0.2592|-0.4814
c|1.2270|-0.6521
d|-1.6687|1.4596
a|-0.2028|-0.6521
b|-0.2549|-0.4814
c|1.2371|-0.6521
col1|col2|col3
----|----|----
c|1.2371|-0.6521
b|-0.2592|-0.4814
c|1.2270|-0.6521
b|-0.2549|-0.4814
a|-0.0784|1.4596
a|-0.2028|-0.6521
d|-1.6687|1.4596
