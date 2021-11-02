# 朴素贝叶斯文本分类预测 (NaiveBayesTextPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.classification.NaiveBayesTextPredictStreamOp

Python 类名：NaiveBayesTextPredictStreamOp


## 功能介绍

* 朴素贝叶斯文本分类算法是一个多分类算法
* 朴素贝叶斯文本分类算法组件支持稀疏、稠密两种数据格式
* 朴素贝叶斯文本分类算法组件支持带样本权重的训练

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
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
          ["$31$0:1.0 1:1.0 2:1.0 30:1.0","1.0  1.0  1.0  1.0", '1'],
          ["$31$0:1.0 1:1.0 2:0.0 30:1.0","1.0  1.0  0.0  1.0", '1'],
          ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
          ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
          ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
          ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
          ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0']
])

batchData = BatchOperator.fromDataframe(df_data, schemaStr='sv string, dv string, label string')

# stream data
streamData = StreamOperator.fromDataframe(df_data, schemaStr='sv string, dv string, label string')
# train op
ns = NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label")
model = batchData.link(ns)
# predict op
predictor = NaiveBayesTextPredictStreamOp(model).setVectorCol("sv").setReservedCols(["sv", "label"]).setPredictionCol("pred")
predictor.linkFrom(streamData).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.NaiveBayesTextTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.classification.NaiveBayesTextPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesTextPredictStreamOpTest {
	@Test
	public void testNaiveBayesTextPredictStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", "1"),
			Row.of("$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", "1"),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", "1"),
			Row.of("$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", "1"),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "0"),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "0"),
			Row.of("$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", "0")
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df_data, "sv string, dv string, label string");
		StreamOperator <?> streamData = new MemSourceStreamOp(df_data, "sv string, dv string, label string");
		BatchOperator <?> ns = new NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label");
		BatchOperator <?> model = batchData.link(ns);
		StreamOperator <?> predictor = new NaiveBayesTextPredictStreamOp(model).setVectorCol("sv").setReservedCols(
			"sv",
			"label").setPredictionCol("pred");
		predictor.linkFrom(streamData).print();
		StreamOperator.execute();
	}
}
```
### 运行结果

sv | label | pred
---|-------|----
"$31$0:1.0 1:1.0 2:1.0 30:1.0"|1|1
"$31$0:1.0 1:1.0 2:0.0 30:1.0"|1|1
"$31$0:1.0 1:0.0 2:1.0 30:1.0"|1|1
"$31$0:1.0 1:0.0 2:1.0 30:1.0"|1|1
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0



