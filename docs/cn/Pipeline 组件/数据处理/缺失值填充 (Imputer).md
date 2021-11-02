# 缺失值填充 (Imputer)
Java 类名：com.alibaba.alink.pipeline.dataproc.Imputer

Python 类名：Imputer


## 功能介绍

填充缺失值，生成的模型可以用于其他数据的预处理过程

支持的填充策略包含最大值，最小值，均值和指定数值，默认为均值填充


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| fillValue | 填充缺失值 | 自定义的填充值。当strategy为value时，读取fillValue的值 | String |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| strategy | 缺失值填充规则 | 缺失值填充的规则，支持mean，max，min或者value。选择value时，需要读取fillValue的值 | String |  | "MEAN" |
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

inOp = BatchOperator.fromDataframe(df, schemaStr='col1 string, col2 double, col3 double')
sinOp = StreamOperator.fromDataframe(df, schemaStr='col1 string, col2 double, col3 double')
                   
model = Imputer()\
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
import com.alibaba.alink.pipeline.dataproc.Imputer;
import com.alibaba.alink.pipeline.dataproc.ImputerModel;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
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
		ImputerModel imputerModel = new Imputer()
			.setSelectedCols(selectedColNames)
            .fit(inOp);
		imputerModel.transform(inOp).print();
		StreamOperator <?> sinOp = new MemSourceStreamOp(df_data, "col1 string, col2 double, col3 int");
		imputerModel.transform(inOp).print();
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
