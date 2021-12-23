# Lasso回归 (LassoRegression)
Java 类名：com.alibaba.alink.pipeline.regression.LassoRegression

Python 类名：LassoRegression


## 功能介绍
* Lasso回归是一个回归算法
* Lasso回归组件支持稀疏、稠密两种数据格式
* Lasso回归组件支持带样本权重的训练

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| lambda | 希腊字母：lambda | 惩罚因子，必选 | Double | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | 100 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| standardization | 是否正则化 | 是否对训练数据做正则化，默认true | Boolean |  | true |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| weightCol | 权重列名 | 权重列对应的列名 | String |  | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  | true |
| optimMethod | 优化方法 | 优化问题求解时选择的优化方法 | String |  | null |
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

batchData = BatchOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')
colnames = ["f0","f1"]
lasso = LassoRegression()\
            .setFeatureCols(colnames)\
            .setLambda(0.1)\
            .setLabelCol("label")\
            .setPredictionCol("pred")
model = lasso.fit(batchData)
model.transform(batchData).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.regression.LassoRegression;
import com.alibaba.alink.pipeline.regression.LassoRegressionModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LassoRegressionTest {
	@Test
	public void testLassoRegression() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(2, 1, 1),
			Row.of(3, 2, 1),
			Row.of(4, 3, 2),
			Row.of(2, 4, 1),
			Row.of(2, 2, 1),
			Row.of(4, 3, 2),
			Row.of(1, 2, 1)
		);
		BatchOperator <?> batchData = new MemSourceBatchOp(df, "f0 int, f1 int, label int");
		String[] colnames = new String[] {"f0", "f1"};
		LassoRegression lasso = new LassoRegression()
			.setFeatureCols(colnames)
			.setLambda(0.1)
			.setLabelCol("label")
			.setPredictionCol("pred");
		LassoRegressionModel model = lasso.fit(batchData);
		model.transform(batchData).print();
	}
}
```
### 运行结果

f0|f1|label|pred
---|---|-----|----
2|1|1|1.1304
3|2|1|1.4047
4|3|2|1.6790
2|4|1|1.1651
2|2|1|1.1420
4|3|2|1.6790
1|2|1|0.8793
