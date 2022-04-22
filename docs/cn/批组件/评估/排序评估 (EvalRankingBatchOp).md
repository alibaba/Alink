# 排序评估 (EvalRankingBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalRankingBatchOp

Python 类名：EvalRankingBatchOp


## 功能介绍
排序评估是对推荐排序算法的预测结果进行效果评估，支持下列评估指标。

#### hitRate	 
<div align=center><img src="https://img.alicdn.com/tfs/TB1FnHAYQY2gK0jSZFgXXc5OFXa-282-136.jpg" ></div>

#### averageReciprocalHitRank
<div align=center><img src="https://img.alicdn.com/tfs/TB1dgAQlwgP7K4jSZFqXXamhVXa-454-164.jpg" ></div>

#### map (Mean Average Precision)
<div align=center><img src="https://img.alicdn.com/tfs/TB1inzAYUY1gK0jSZFCXXcwqXXa-494-74.jpg"></div>

#### ndcgArray (Normalized Discounted Cumulative Gain)
$$ MSE=\dfrac{1}{N}\sum_{i=1}^{N}(f_i-y_i)^2 $$


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
    ["{\"object\":\"[1, 6, 2, 7, 8, 3, 9, 10, 4, 5]\"}", "{\"object\":\"[1, 2, 3, 4, 5]\"}"],
    ["{\"object\":\"[4, 1, 5, 6, 2, 7, 3, 8, 9, 10]\"}", "{\"object\":\"[1, 2, 3]\"}"],
    ["{\"object\":\"[1, 2, 3, 4, 5]\"}", "{\"object\":\"[]\"}"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='pred string, label string')

metrics = EvalRankingBatchOp().setPredictionCol('pred').setLabelCol('label').linkFrom(inOp).collectMetrics()
print(metrics)
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalRankingBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.RankingMetrics;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EvalRankingBatchOpTest {
	@Test
	public void testEvalRankingBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("{\"object\":\"[1, 6, 2, 7, 8, 3, 9, 10, 4, 5]\"}", "{\"object\":\"[1, 2, 3, 4, 5]\"}"),
			Row.of("{\"object\":\"[4, 1, 5, 6, 2, 7, 3, 8, 9, 10]\"}", "{\"object\":\"[1, 2, 3]\"}"),
			Row.of("{\"object\":\"[1, 2, 3, 4, 5]\"}", "{\"object\":\"[]\"}")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "pred string, label string");
		RankingMetrics metrics = new EvalRankingBatchOp().setPredictionCol("pred").setLabelCol("label").linkFrom(inOp)
			.collectMetrics();
		System.out.println(metrics.toString());
	}
}
```

### 运行结果
```
-------------------------------- Metrics: --------------------------------
microPrecision:0.32
averageReciprocalHitRank:0.5
precision:0.2667
accuracy:0.2667
f1:0.3761
hitRate:0.6667
microRecall:1
microF1:0.4848
subsetAccuracy:0
recall:0.6667
map:0.355
hammingLoss:0.5667
```

