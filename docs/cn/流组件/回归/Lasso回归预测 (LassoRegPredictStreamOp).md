# Lasso回归预测 (LassoRegPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.regression.LassoRegPredictStreamOp

Python 类名：LassoRegPredictStreamOp


## 功能介绍
Lasso回归算法是由1996年Robert Tibshirani首次提出。是一种经典的回归算法。Lasso回归组件支持稀疏、稠密两种数据格式，并且支持带权重样本训练。

### 算法原理
Lasso回归算法通过构造一个惩罚函数得到一个较为精炼的模型，使得它压缩一些回归系数，即强制系数绝对值之和小于某个固定值；同时设定一些回归系数为零。因此保留了子集收缩的优点，是一种处理具有复共线性数据的有偏估计。

### 算法使用
Lasso回归模型应用领域和线性回归类似，经常被用来做一些数值型变量的预测，类似房价预测、销售量预测、贷款额度预测、温度预测、适度预测等。

### 文献或出处
[1] Tibshirani, Robert. "Regression shrinkage and selection via the lasso." Journal of the Royal Statistical Society: Series B (Methodological) 58.1 (1996): 267-288.

[2] https://baike.baidu.com/item/LASSO/20366865?fr=aladdin



## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
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
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])

batchData = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 double, label double')
streamData = StreamOperator.fromDataframe(df, schemaStr='f0 double, f1 double, label double')

colnames = ["f0","f1"]
lasso = LassoRegTrainBatchOp()\
            .setLambda(0.1)\
            .setFeatureCols(colnames)\
            .setLabelCol("label")

model = batchData.link(lasso)

predictor = LassoRegPredictStreamOp(model)\
            .setPredictionCol("pred")

predictor.linkFrom(streamData).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.regression.LassoRegTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.regression.LassoRegPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LassoRegPredictStreamOpTest {
	@Test
	public void testLassoRegPredictStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(2.0, 1.0, 1.0),
			Row.of(3.0, 2.0, 1.0),
			Row.of(4.0, 3.0, 2.0),
			Row.of(2.0, 4.0, 1.0),
			Row.of(2.0, 2.0, 1.0),
			Row.of(4.0, 3.0, 2.0),
			Row.of(1.0, 2.0, 1.0)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "f0 double, f1 double, label double");
		StreamOperator <?> streamData = new MemSourceStreamOp(df, "f0 double, f1 double, label double");
		String[] colnames = new String[] {"f0", "f1"};
		BatchOperator <?> lasso = new LassoRegTrainBatchOp()
			.setLambda(0.1)
			.setFeatureCols(colnames)
			.setLabelCol("label");
		BatchOperator <?> model = batchData.link(lasso);
		StreamOperator <?> predictor = new LassoRegPredictStreamOp(model)
			.setPredictionCol("pred");
		predictor.linkFrom(streamData).print();
		StreamOperator.execute();
	}
}
```
### 运行结果
f0|f1|label|pred
---|---|-----|----
3.0000|2.0000|1.0000|1.4047
4.0000|3.0000|2.0000|1.6790
2.0000|4.0000|1.0000|1.1651
2.0000|1.0000|1.0000|1.1304
2.0000|2.0000|1.0000|1.1420
1.0000|2.0000|1.0000|0.8793
4.0000|3.0000|2.0000|1.6790
