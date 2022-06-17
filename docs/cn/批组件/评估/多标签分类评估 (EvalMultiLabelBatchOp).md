# 多标签分类评估 (EvalMultiLabelBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalMultiLabelBatchOp

Python 类名：EvalMultiLabelBatchOp


## 功能介绍
对多标签分类算法的预测结果进行效果评估。

### 算法原理

在多标签分类问题中，每个样本点 $i$ 所属标签集合记为 $L_i$，模型预测给出的预测集合记为 $P_i$；样本点总数记为 $N$。

#### Precision

$\frac{1}{N} \sum_{i=0}^{N-1} \frac{\left|P_i \cap L_i\right|}{\left|P_i\right|}$

#### Recall

$\frac{1}{N} \sum_{i=0}^{N-1} \frac{\left|L_i \cap P_i\right|}{\left|L_i\right|}$

#### Accuracy

$\frac{1}{N} \sum_{i=0}^{N - 1} \frac{\left|L_i \cap P_i \right|}{\left|L_i\right| + \left|P_i\right| - \left|L_i \cap P_i \right|}$

#### Hamming Loss, HL

$\frac{1}{N \cdot \left|L\right|} \sum_{i=0}^{N - 1} \left|L_i\right| + \left|P_i\right| - 2\left|L_i \cap P_i\right|$

#### Subset Accuracy, SA

$\frac{1}{N}\sum_{i=0}^{N-1}I[L_i =P_i]$

这里 $I[\cdot]$ 是指示函数，内部条件满足时值为1，其他时候为0。

#### F1 Measure

$\frac{1}{N} \sum_{i=0}^{N-1} 2 \frac{\left|P_i \cap L_i\right|}{\left|P_i\right| \cdot \left|L_i\right|}$

#### Micro Precision

$\frac{TP}{TP + FP}=\frac{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right|}{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right| + \sum_{i=0}^{N-1} \left|P_i - L_i\right|}$

#### Micro Recall

$\frac{TP}{TP + FN}=\frac{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right|}{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right| + \sum_{i=0}^{N-1} \left|L_i - P_i\right|}$

#### Micro F1

$2 \cdot \frac{TP}{2 \cdot TP + FP + FN}=2 \cdot \frac{\sum_{i=0}^{N-1} \left|P_i \cap L_i\right|}{2 \cdot \sum_{i=0}^{N-1} \left|P_i \cap L_i\right| + \sum_{i=0}^{N-1} \left|L_i - P_i\right| + \sum_{i=0}^{N-1} \left|P_i - L_i\right|}$

### 使用方式

该组件通常接多标签分类预测算法的输出端。

使用时，需要通过参数 `labelCol` 指定预测标签列，参数 `predictionCol` 和 `predictionCol` 指定预测结果列。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| labelRankingInfo | Object列列名 | Object列列名 | String |  |  | "object" |
| predictionRankingInfo | Object列列名 | Object列列名 | String |  |  | "object" |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["{\"object\":\"[0.0, 1.0]\"}", "{\"object\":\"[0.0, 2.0]\"}"],
    ["{\"object\":\"[0.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"],
    ["{\"object\":\"[]\"}", "{\"object\":\"[0.0]\"}"],
    ["{\"object\":\"[2.0]\"}", "{\"object\":\"[2.0]\"}"],
    ["{\"object\":\"[2.0, 0.0]\"}", "{\"object\":\"[2.0, 0.0]\"}"],
    ["{\"object\":\"[0.0, 1.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"],
    ["{\"object\":\"[1.0]\"}", "{\"object\":\"[1.0, 2.0]\"}"]
])

source = BatchOperator.fromDataframe(df, "pred string, label string")

evalMultiLabelBatchOp: EvalMultiLabelBatchOp = EvalMultiLabelBatchOp().setLabelCol("label").setPredictionCol("pred").linkFrom(source)
metrics = evalMultiLabelBatchOp.collectMetrics()
print(metrics)
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalMultiLabelBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.MultiLabelMetrics;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EvalMultiLabelBatchOpTest {
	@Test
	public void testEvalMultiLabelBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("{\"object\":\"[0.0, 1.0]\"}", "{\"object\":\"[0.0, 2.0]\"}"),
			Row.of("{\"object\":\"[0.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"),
			Row.of("{\"object\":\"[]\"}", "{\"object\":\"[0.0]\"}"),
			Row.of("{\"object\":\"[2.0]\"}", "{\"object\":\"[2.0]\"}"),
			Row.of("{\"object\":\"[2.0, 0.0]\"}", "{\"object\":\"[2.0, 0.0]\"}"),
			Row.of("{\"object\":\"[0.0, 1.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"),
			Row.of("{\"object\":\"[1.0]\"}", "{\"object\":\"[1.0, 2.0]\"}")
		);
		BatchOperator <?> source = new MemSourceBatchOp(df, "pred string, label string");
		EvalMultiLabelBatchOp evalMultiLabelBatchOp =
			new EvalMultiLabelBatchOp().setLabelCol("label").setPredictionCol(
			"pred").linkFrom(source);
		MultiLabelMetrics metrics = evalMultiLabelBatchOp.collectMetrics();
		System.out.println(metrics.toString());
	}
}
```

### 运行结果
```
-------------------------------- Metrics: --------------------------------
microPrecision:0.7273
microF1:0.6957
subsetAccuracy:0.2857
precision:0.6667
recall:0.6429
accuracy:0.5476
f1:0.6381
microRecall:0.6667
hammingLoss:0.3333
```
