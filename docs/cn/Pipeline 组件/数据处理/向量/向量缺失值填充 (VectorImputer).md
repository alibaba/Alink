# 向量缺失值填充 (VectorImputer)
Java 类名：com.alibaba.alink.pipeline.dataproc.vector.VectorImputer

Python 类名：VectorImputer


## 功能介绍
- 训练Vector 缺失值填充组件的模型，输出模型。
- 填充策略支持最大值，最小值，均值和默认值4种策略，默认为均值。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| fillValue | 填充缺失值 | 自定义的填充值。当strategy为value时，读取fillValue的值 | Double |  |  | null |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| strategy | 缺失值填充规则 | 缺失值填充的规则，支持mean，max，min或者value。选择value时，需要读取fillValue的值 | String |  | "MEAN", "MIN", "MAX", "VALUE" | "MEAN" |
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
    ["1:3,2:4,4:7", 1],
    ["1:3,2:NaN", 3],
    ["2:4,4:5", 4]])
data = BatchOperator.fromDataframe(df, schemaStr="vec string, id bigint")
vecFill = VectorImputer().setSelectedCol("vec").setOutputCol("vec1")
model = vecFill.fit(data)
model.transform(data).collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorImputer;
import com.alibaba.alink.pipeline.dataproc.vector.VectorImputerModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorImputerTest {
	@Test
	public void testVectorImputer() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1:3,2:4,4:7", 1),
			Row.of("1:3,2:NaN", 3)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "vec string, id int");
		VectorImputer vecFill = new VectorImputer().setSelectedCol("vec").setOutputCol("vec1");
		VectorImputerModel model = vecFill.fit(data);
		model.transform(data).print();
	}
}
```
### 运行结果


| vec         | id   | vec1              |
| ----------- | ---- | ----------------- |
| 1:3,2:4,4:7 | 1    | 1:3.0 2:4.0 4:7.0 |
| 1:3,2:NaN   | 3    | 1:3.0 2:4.0       |
| 2:4,4:5     | 4    | 2:4.0 4:5.0       |
