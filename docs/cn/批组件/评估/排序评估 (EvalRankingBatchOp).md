# 排序评估 (EvalRankingBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalRankingBatchOp

Python 类名：EvalRankingBatchOp


## 功能介绍
对推荐排序算法的预测结果进行效果评估。

### 算法原理

在排序问题中，可以忽略顺序，将问题看作多标签分类问题。这样多标签分类评估中的指标都能使用。

在考虑顺序时，假设有$M$个用户，每个用户的真实标签集合为 $D_i = \left\{d_0, d_1, ..., d_{N-1}\right\}$，模型推荐的集合为 $R_i = \left[r_0, r_1, ..., r_{Q-1}\right]$，按相关程序递减排序。
定义$rel_D(r)$在满足$r\in D$时为1，否则为0。 
那么，还可以支持以下评估指标。

#### Hit Rate

$HitRate=\frac{1}{M}\sum_{i=0}^{M-1} \sum_{j=0}^{\left|D\right| - 1}rel_{\{d_0\}}(R_i(j))$

#### Average Reciprocal Hit Rank

$ARHR=\frac{1}{M}\sum_{i=0}^{M-1} \sum_{j=0}^{\left|D\right| - 1}\frac{1}{j+1}rel_{\{d_0\}}(R_i(j))$

#### Precision at k

$p(k)=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{k} \sum_{j=0}^{\min(\left|D\right|, k) - 1} rel_{D_i}(R_i(j))}$

这里 $k$ 是一个参数。

#### Recall at k

$recall(k)=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{N} \sum_{j=0}^{\min(\left|D\right|, k) - 1} rel_{D_i}(R_i(j))}$

这里 $k$ 是一个参数。

#### MAP (Mean Average Precision)

$MAP=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{\left|D_i\right|} \sum_{j=0}^{Q-1} \frac{rel_{D_i}(R_i(j))}{j + 1}}$

#### NDCG at k (Normalized Discounted Cumulative Gain)

$NDCG(k)=\frac{1}{M} \sum_{i=0}^{M-1} {\frac{1}{IDCG(D_i, k)}\sum_{j=0}^{n-1} \frac{rel_{D_i}(R_i(j))}{\ln(j+2)}},$

其中，$n = \min\left(\max\left(|R_i|,|D_i|\right),k\right)$， $IDCG(D, k) = \sum_{j=0}^{\min(\left|D\right|, k) - 1} \frac{1}{\ln(j+2)}$。

这里 $k$ 是一个参数。

### 使用方式

该组件通常接推荐排序预测算法的输出端。

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

