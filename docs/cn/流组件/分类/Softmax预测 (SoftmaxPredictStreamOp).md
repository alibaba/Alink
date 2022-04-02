# Softmax预测 (SoftmaxPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.classification.SoftmaxPredictStreamOp

Python 类名：SoftmaxPredictStreamOp


## 功能介绍
Softmax算法是Logistic回归算法的推广，Logistic回归主要是用来处理二分类问题，而Softmax回归则是处理多分类问题。

### 算法原理
面对多分类问题，建立代价函数，然后通过优化方法迭代求解出最优的模型参数。具体原理可参考文献。

### 算法使用

该算法经常应用到多分类问题中，类似情感分析、手写字识别等问题都可以使用Softmax算法，该算法支持稀疏和稠密两种输入样本。

- 备注 ：该组件训练的时候 FeatureCols 和 VectorCol 是两个互斥参数，只能有一个参数来描述算法的输入特征。

### 文献或出处
[1] Brown, Peter F., et al. "Class-based n-gram models of natural language." Computational linguistics 18.4 (1992): 467-480.

[2] Goodman, Joshua. "Classes for fast maximum entropy training." 2001 IEEE International Conference on Acoustics, Speech, and Signal Processing. Proceedings (Cat. No. 01CH37221). Vol. 1. IEEE, 2001.

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

df_data = pd.DataFrame([
       [2, 1, 1],
       [3, 2, 1],
       [4, 3, 2],
       [2, 4, 1],
       [2, 2, 1],
       [4, 3, 2],
       [1, 2, 1],
       [5, 3, 3]
])

batchData = BatchOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')
dataTest = StreamOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')
colnames = ["f0","f1"]
lr = SoftmaxTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(lr)

predictor = SoftmaxPredictStreamOp(model).setPredictionCol("pred")
predictor.linkFrom(dataTest).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.SoftmaxTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.SoftmaxPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SoftmaxPredictStreamOpTest {
	@Test
	public void testSoftmaxPredictStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1),
			Row.of(5, 3, 3)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df_data, "f0 int, f1 int, label int");
		StreamOperator <?> dataTest = new MemSourceStreamOp(df_data, "f0 int, f1 int, label int");
		String[] colnames = new String[] {"f0", "f1"};
		BatchOperator <?> lr = new SoftmaxTrainBatchOp().setFeatureCols(colnames).setLabelCol("label");
		BatchOperator <?> model = batchData.link(lr);
		StreamOperator <?> predictor = new SoftmaxPredictStreamOp(model).setPredictionCol("pred");
		predictor.linkFrom(dataTest).print();
		StreamOperator.execute();
	}
}
```
### 运行结果
f0 | f1 | label | pred 
---|----|-------|-----
2|1|1|1
3|2|1|1
4|3|2|2
2|4|1|1
2|2|1|1
4|3|2|2
1|2|1|1
5|3|3|3



