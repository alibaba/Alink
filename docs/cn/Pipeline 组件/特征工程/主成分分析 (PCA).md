# 主成分分析 (PCA)
Java 类名：com.alibaba.alink.pipeline.feature.PCA

Python 类名：PCA


## 功能介绍

主成分分析，是考察多个变量间相关性一种多元统计方法，研究如何通过少数几个主成分来揭示多个变量间的内部结构，即从原始变量中导出少数几个主成分，使它们尽可能多地保留原始变量的信息，且彼此间互不相关，作为新的综合指标。详细介绍请见维基百科链接[wiki](https://en.wikipedia.org/wiki/Principal_component_analysis)。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| k | 降维后的维度 | 降维后的维度 | Integer | ✓ | x >= 1 |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| calculationType | 计算类型 | 计算类型，包含"CORR", "COV"两种。 | String |  | "CORR", "COV" | "CORR" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| selectedCols | 选中的列名数组 | 计算列对应的列名列表 | String[] |  |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  |  | null |
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
        [0.0,0.0,0.0],
        [0.1,0.2,0.1],
        [0.2,0.2,0.8],
        [9.0,9.5,9.7],
        [9.1,9.1,9.6],
        [9.2,9.3,9.9]
])

# batch source 
inOp = BatchOperator.fromDataframe(df, schemaStr='x1 double, x2 double, x3 double')

pca = PCA().setK(2).setSelectedCols(["x1","x2","x3"]).setPredictionCol("pred")

# train
model = pca.fit(inOp)

# batch predict
model.transform(inOp).print()

# stream predict
inStreamOp = StreamOperator.fromDataframe(df, schemaStr='x1 double, x2 double, x3 double')

model.transform(inStreamOp).print()

StreamOperator.execute()
```
### Java 代码

```java
package javatest.com.alibaba.alink.pipeline.feature;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.feature.PCA;
import com.alibaba.alink.pipeline.feature.PCAModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PcaTest {

	@Test
	public void testPca() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0.0, 0.0, 0.0),
			Row.of(0.1, 0.2, 0.1),
			Row.of(0.2, 0.2, 0.8),
			Row.of(9.0, 9.5, 9.7),
			Row.of(9.1, 9.1, 9.6),
			Row.of(9.2, 9.3, 9.9)
		);

		BatchOperator <?> inOp = new MemSourceBatchOp(df, "x1 double, x2 double, x3 double");
		MemSourceStreamOp inStreamOp = new MemSourceStreamOp(df, "x1 double, x2 double, x3 double");

		PCA pca = new PCA()
			.setK(2)
			.setSelectedCols(new String[] {"x1", "x2", "x3"}).setPredictionCol("pred");

		PCAModel model = pca.fit(inOp);

		model.transform(inOp).print();

		model.transform(inStreamOp).print();

		StreamOperator.execute();
	}
}

```
#### 结果

x1|x2|x3|pred
---|---|---|----
0.0000|0.0000|0.0000|-1.6404909810453345 -0.0251812826908675
0.1000|0.2000|0.1000|-1.5946357760302712 -0.037364200387782764
0.2000|0.2000|0.8000|-1.5048402139720405 0.06485201225195414
9.0000|9.5000|9.7000|1.587547449494739 -0.02506612043660217
9.1000|9.1000|9.6000|1.5421273389336387 0.0022882493013524074
9.2000|9.3000|9.9000|1.6102921826192689 0.020471341961945777



