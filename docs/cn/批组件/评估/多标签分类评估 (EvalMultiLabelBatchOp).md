# 多标签分类评估 (EvalMultiLabelBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalMultiLabelBatchOp

Python 类名：EvalMultiLabelBatchOp


## 功能介绍
多label分类评估是对多label分类算法的预测结果进行效果评估，支持下列评估指标。

#### subsetAccuracy
<div align=center><img src="https://img.alicdn.com/tfs/TB1QHzHYRr0gK0jSZFnXXbRRXXa-119-31.jpg" ></div>

#### hammingLoss
<div align=center><img src="https://img.alicdn.com/tfs/TB1OtDqYLb2gK0jSZK9XXaEgFXa-249-30.jpg" ></div>

#### accuracy
<div align=center><img src="https://img.alicdn.com/tfs/TB1EAHtYKH2gK0jSZJnXXaT1FXa-160-36.jpg" ></div>

#### microPrecision
<div aligh=center><img src="https://img.alicdn.com/tfs/TB1eq_nYFY7gK0jSZKzXXaikpXa-212-45.jpg" ></div>

#### microRecall
<div align=center><img src="https://img.alicdn.com/tfs/TB1CDruYUH1gK0jSZSyXXXtlpXa-214-44.jpg" ></div>

#### microF1
<div align=center><img src="https://img.alicdn.com/tfs/TB1dAzFYUT1gK0jSZFrXXcNCXXa-370-50.jpg" ></div>

#### precision
<div align=center><img src="https://img.alicdn.com/tfs/TB1H12oYFY7gK0jSZKzXXaikpXa-113-34.jpg" ></div>

#### recall
<div align=center><img src="https://img.alicdn.com/tfs/TB1LuKilZVl614jSZKPXXaGjpXa-110-36.jpg" ></div>

#### f1
$$ explained Variance=\dfrac{SSR}{N} $$


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| labelRankingInfo | Object列列名 | Object列列名 | String |  | "object" |
| predictionRankingInfo | Object列列名 | Object列列名 | String |  | "object" |

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
