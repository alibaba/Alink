# 朴素贝叶斯文本分类器 (NaiveBayesTextClassifier)
Java 类名：com.alibaba.alink.pipeline.classification.NaiveBayesTextClassifier

Python 类名：NaiveBayesTextClassifier


## 功能介绍

* 朴素贝叶斯文本分类算法是一个多分类算法
* 朴素贝叶斯文本分类算法组件支持稀疏、稠密两种数据格式
* 朴素贝叶斯文本分类算法组件支持带样本权重的训练

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| smoothing | 算法参数 | 光滑因子，默认为1.0 | Double |  | 1.0 |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| modelType | 模型类型 | 取值为 Multinomial 或 Bernoulli | String |  | "Multinomial" |
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
# pipeline
model = NaiveBayesTextClassifier().setVectorCol("sv").setLabelCol("label").setReservedCols(["sv", "label"]).setPredictionCol("pred")
model.fit(batchData).transform(batchData).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.classification.NaiveBayesTextClassifier;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NaiveBayesTextClassifierTest {
	@Test
	public void testNaiveBayesTextClassifier() throws Exception {
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
		NaiveBayesTextClassifier model = new NaiveBayesTextClassifier().setVectorCol("sv").setLabelCol("label")
			.setReservedCols("sv", "label").setPredictionCol("pred");
		model.fit(batchData).transform(batchData).print();
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
