# One-Class SVM异常检测 (OcsvmOutlierBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.OcsvmOutlierBatchOp

Python 类名：OcsvmOutlierBatchOp


## 功能介绍
与传统SVM不同的是，one-class SVM是一种非监督的学习算法，经常被用来做异常点检测。在该算法的训练集中只有一类positive（或者negative）的数据，而没有（或存在极少量）另外一类，通常称其为异常点。该算法需要学习（learn）的就是边界（boundary），而不是最大间隔（maximum margin），通过边界对异常点进行预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| coef0 | Kernel函数的相关参数coef0 |  Kernel函数的相关参数，只有在POLY和SIGMOID时起作用。 | Double |  |  | 0.0 |
| degree | 多项式阶数 | 多项式的阶数，默认2 | Integer |  | x >= 1 | 2 |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | x >= 0.0 | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| gamma | Kernel函数的相关参数gamma | Kernel函数的相关参数，只在 RBF, POLY 和 SIGMOID 时起作用. 如果不设置默认取 1/d，d为特征维度. | Double |  |  | -1.0 |
| groupCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| kernelType | 核函数类型 | 核函数类型，可取为"RBF"，"POLY"，"SIGMOID"，"LINEAR" | String |  | "RBF", "POLY", "SIGMOID", "LINEAR" | "RBF" |
| maxOutlierNumPerGroup | 每组最大异常点数目 | 每组最大异常点数目 | Integer |  |  |  |
| maxOutlierRatio | 最大异常点比例 | 算法检测异常点的最大比例 | Double |  |  |  |
| maxSampleNumPerGroup | 每组最大样本数目 | 每组最大样本数目 | Integer |  |  |  |
| nu | 异常点比例上界参数nu | 该参数取值范围是(0,1)，该值与支持向量的数目正向相关。 | Double |  |  | 0.01 |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| tensorCol | tensor列 | tensor列 | String |  | 所选列类型为 [BOOL_TENSOR, BYTE_TENSOR, DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR, STRING, STRING_TENSOR, TENSOR, UBYTE_TENSOR] | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
[0.730967787376657,0.24053641567148587,0.6374174253501083,0.5504370051176339],
[0.7308781907032909,0.41008081149220166,0.20771484130971707,0.3327170559595112],
[0.7311469360199058,0.9014476240300544,0.49682259343089075,0.9858769332362016],
[0.731057369148862,0.07099203475193139,0.06712000939049956,0.768156984078079],
[0.7306094602878371,0.9187140138555101,0.9186071189908658,0.6795571637816596],
[0.730519863614471,0.08825840967622589,0.4889045498516358,0.461837214623537],
[0.7307886238322471,0.5796252073129174,0.7780122870716483,0.11499709190022733],
[0.7306990420600421,0.7491696031336331,0.34830970303125697,0.8972771427421047]])

# load data
data = BatchOperator.fromDataframe(df, schemaStr="x1 double, x2 double, x3 double, x4 double")

OcsvmOutlierBatchOp() \
			.setFeatureCols(["x1", "x2", "x3", "x4"]) \
			.setGamma(0.5) \
			.setNu(0.1) \
			.setKernelType("RBF") \
			.setPredictionCol("pred").linkFrom(data).print();
```

### Java 代码
```java
package com.alibaba.alink.operator.batch.outlier;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

public class OcsvmBatchOpTest {
	@Test
	public void testOutlier() throws Exception {

		BatchOperator <?> data = new MemSourceBatchOp(
			new Object[][] {
				{0.730967787376657, 0.24053641567148587, 0.6374174253501083, 0.5504370051176339},
				{0.7308781907032909, 0.41008081149220166, 0.20771484130971707, 0.3327170559595112},
				{0.7311469360199058, 0.9014476240300544, 0.49682259343089075, 0.9858769332362016},
				{0.731057369148862, 0.07099203475193139, 0.06712000939049956, 0.768156984078079},
				{0.7306094602878371, 0.9187140138555101, 0.9186071189908658, 0.6795571637816596},
				{0.730519863614471, 0.08825840967622589, 0.4889045498516358, 0.461837214623537},
				{0.7307886238322471, 0.5796252073129174, 0.7780122870716483, 0.11499709190022733},
				{0.7306990420600421, 0.7491696031336331, 0.34830970303125697, 0.8972771427421047}
			},
			new String[] {"x1", "x2", "x3", "x4"});

		new OcsvmOutlierBatchOp()
			.setFeatureCols("x1", "x2", "x3", "x4")
			.setGamma(0.5)
			.setNu(0.2)
			.setKernelType("RBF")
			.setPredictionCol("pred").linkFrom(data).print();
	}
}
```
### 运行结果
x1|x2|x3|x4|pred
---|---|---|---|----
0.7310|0.2405|0.6374|0.5504|false
0.7309|0.4101|0.2077|0.3327|false
0.7311|0.9014|0.4968|0.9859|false
0.7311|0.0710|0.0671|0.7682|false
0.7306|0.9187|0.9186|0.6796|true
0.7305|0.0883|0.4889|0.4618|false
0.7308|0.5796|0.7780|0.1150|false
0.7307|0.7492|0.3483|0.8973|false
