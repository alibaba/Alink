# 多层感知机分类预测 (MultilayerPerceptronPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.classification.MultilayerPerceptronPredictStreamOp

Python 类名：MultilayerPerceptronPredictStreamOp


## 功能介绍
基于多层感知机模型，进行分类预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
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
    [5,2,3.5,1,'Iris-versicolor'],
    [5.1,3.7,1.5,0.4,'Iris-setosa'],
    [6.4,2.8,5.6,2.2,'Iris-virginica'],
    [6,2.9,4.5,1.5,'Iris-versicolor'],
    [4.9,3,1.4,0.2,'Iris-setosa'],
    [5.7,2.6,3.5,1,'Iris-versicolor'],
    [4.6,3.6,1,0.2,'Iris-setosa'],
    [5.9,3,4.2,1.5,'Iris-versicolor'],
    [6.3,2.8,5.1,1.5,'Iris-virginica'],
    [4.7,3.2,1.3,0.2,'Iris-setosa'],
    [5.1,3.3,1.7,0.5,'Iris-setosa'],
    [5.5,2.4,3.8,1.1,'Iris-versicolor'],
])

train_data = BatchOperator.fromDataframe(df, schemaStr='sepal_length double, sepal_width double, petal_length double, petal_width double, category string')

test_data = StreamOperator.fromDataframe(df, schemaStr='sepal_length double, sepal_width double, petal_length double, petal_width double, category string')

mlpc = MultilayerPerceptronTrainBatchOp() \
    .setFeatureCols(["sepal_length", "sepal_width", "petal_length", "petal_width"]) \
    .setLabelCol("category") \
    .setLayers([4, 5, 3]) \
    .setMaxIter(10)

model = mlpc.linkFrom(train_data)

predictor = MultilayerPerceptronPredictStreamOp(model)\
  .setPredictionCol('p')

predictor.linkFrom(test_data).print()
StreamOperator.execute()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.MultilayerPerceptronTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.MultilayerPerceptronPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MultilayerPerceptronPredictStreamOpTest {
	@Test
	public void testMultilayerPerceptronPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(5.0, 2.0, 3.5, 1.0, "Iris-versicolor"),
			Row.of(5.1, 3.7, 1.5, 0.4, "Iris-setosa"),
			Row.of(6.4, 2.8, 5.6, 2.2, "Iris-virginica"),
			Row.of(6.0, 2.9, 4.5, 1.5, "Iris-versicolor"),
			Row.of(4.9, 3.0, 1.4, 0.2, "Iris-setosa"),
			Row.of(5.7, 2.6, 3.5, 1.0, "Iris-versicolor"),
			Row.of(4.6, 3.6, 1.0, 0.2, "Iris-setosa"),
			Row.of(5.9, 3.0, 4.2, 1.5, "Iris-versicolor"),
			Row.of(6.3, 2.8, 5.1, 1.5, "Iris-virginica"),
			Row.of(4.7, 3.2, 1.3, 0.2, "Iris-setosa"),
			Row.of(5.1, 3.3, 1.7, 0.5, "Iris-setosa"),
			Row.of(5.5, 2.4, 3.8, 1.1, "Iris-versicolor")
		);
		BatchOperator <?> train_data = new MemSourceBatchOp(df,
			"sepal_length double, sepal_width double, petal_length double, petal_width double, category string");
		StreamOperator <?> test_data = new MemSourceStreamOp(df,
			"sepal_length double, sepal_width double, petal_length double, petal_width double, category string");
		BatchOperator <?> mlpc = new MultilayerPerceptronTrainBatchOp()
			.setFeatureCols("sepal_length", "sepal_width", "petal_length", "petal_width")
			.setLabelCol("category")
			.setLayers(new int[] {4, 5, 3})
			.setMaxIter(10);
		BatchOperator <?> model = mlpc.linkFrom(train_data);
		StreamOperator <?> predictor = new MultilayerPerceptronPredictStreamOp(model)
			.setPredictionCol("p");
		predictor.linkFrom(test_data).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

sepal_length|sepal_width|petal_length|petal_width|category|p
------------|-----------|------------|-----------|--------|---
5.0000|2.0000|3.5000|1.0000|Iris-versicolor|Iris-versicolor
5.1000|3.7000|1.5000|0.4000|Iris-setosa|Iris-versicolor
6.4000|2.8000|5.6000|2.2000|Iris-virginica|Iris-versicolor
6.0000|2.9000|4.5000|1.5000|Iris-versicolor|Iris-versicolor
4.9000|3.0000|1.4000|0.2000|Iris-setosa|Iris-versicolor
5.7000|2.6000|3.5000|1.0000|Iris-versicolor|Iris-versicolor
4.6000|3.6000|1.0000|0.2000|Iris-setosa|Iris-setosa
5.9000|3.0000|4.2000|1.5000|Iris-versicolor|Iris-versicolor
6.3000|2.8000|5.1000|1.5000|Iris-virginica|Iris-versicolor
4.7000|3.2000|1.3000|0.2000|Iris-setosa|Iris-versicolor
5.1000|3.3000|1.7000|0.5000|Iris-setosa|Iris-versicolor
5.5000|2.4000|3.8000|1.1000|Iris-versicolor|Iris-versicolor
