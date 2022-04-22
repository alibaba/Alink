# 向量归一化 (VectorMinMaxScaler)
Java 类名：com.alibaba.alink.pipeline.dataproc.vector.VectorMinMaxScaler

Python 类名：VectorMinMaxScaler


## 功能介绍

- vector归一化是对vector数据进行归一的组件, 将数据归一到min和max之间。
- 计算公式为x_scaled = (x - eMin) / (eMax - eMin) * (maxV - minV) + minV 其中maxV和minV为用户设定的，默认值为1和0
- 该组件提供训练功能，生成的Vector归一化模型供预测使用

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| max | 归一化的上界 | 归一化的上界 | Double |  |  | 1.0 |
| min | 归一化的下界 | 归一化的下界 | Double |  |  | 0.0 |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
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
    ["a", "10.0, 100"],
    ["b", "-2.5, 9"],
    ["c", "100.2, 1"],
    ["d", "-99.9, 100"],
    ["a", "1.4, 1"],
    ["b", "-2.2, 9"],
    ["c", "100.9, 1"]
])
data = BatchOperator.fromDataframe(df, schemaStr="col string, vec string")
res = VectorMinMaxScaler()\
           .setSelectedCol("vec")
model = res.fit(data)
model.transform(data).collectToDataframe()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.vector.VectorMinMaxScaler;
import com.alibaba.alink.pipeline.dataproc.vector.VectorMinMaxScalerModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class VectorMinMaxScalerTest {
	@Test
	public void testVectorMinMaxScaler() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", "10.0, 100"),
			Row.of("b", "-2.5, 9"),
			Row.of("c", "100.2, 1"),
			Row.of("d", "-99.9, 100"),
			Row.of("a", "1.4, 1"),
			Row.of("b", "-2.2, 9"),
			Row.of("c", "100.9, 1")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "col string, vec string");
		VectorMinMaxScaler res = new VectorMinMaxScaler()
			.setSelectedCol("vec");
		VectorMinMaxScalerModel model = res.fit(data);
		model.transform(data).print();
	}
}
```
### 运行结果

col1|vec
----|---
a|0.5473107569721115,1.0
b|0.4850597609561753,0.08080808080808081
c|0.9965139442231076,0.0
d|0.0,1.0
a|0.5044820717131474,0.0
b|0.4865537848605578,0.08080808080808081
c|1.0,0.0
