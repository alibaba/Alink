# 二分类评估 (EvalBinaryClassBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp

Python 类名：EvalBinaryClassBatchOp


## 功能介绍
二分类评估是对二分类算法的预测结果进行效果评估。

支持Roc曲线，LiftChart曲线，K-S曲线，Recall-Precision曲线绘制。

流式的实验支持累计统计和窗口统计，除却上述四条曲线外，还给出Auc/Kappa/Accuracy/Logloss随时间的变化曲线。

给出整体的评估指标包括：AUC、K-S、PRC, 不同阈值下的Precision、Recall、F-Measure、Sensitivity、Accuracy、Specificity和Kappa。

#### 混淆矩阵
<div align=center><img src="https://img.alicdn.com/tfs/TB1lCMOaW67gK0jSZFHXXa9jVXa-1582-688.jpg" height="50%" width="50%"></div>

#### Roc曲线

横坐标：FPR 

纵坐标：TPR

#### AUC

Roc曲线下面的面积

#### K-S

横坐标：阈值

纵坐标：TPR和FPR

#### KS

K-S曲线两条纵轴的最大差值

#### Recall-Precision曲线

横坐标：Recall

纵坐标：Precision

#### PRC

Recall-Precision曲线下面的面积

#### 提升曲线

横坐标：$$ \dfrac{TP + FP}{total} $$


纵坐标：TP

#### Precision
$$ Precision = \dfrac{TP}{TP + FP} $$


#### Recall
$$ Recall = \dfrac{TP}{TP + FN} $$


#### F-Measure
$$ F1=\dfrac{2TP}{2TP+FP+FN}=\dfrac{2\cdot Precision \cdot Recall}{Precision+Recall} $$


#### Sensitivity
$$ Sensitivity=\dfrac{TP}{TP+FN} $$


#### Accuracy
$$ Accuray=\dfrac{TP + TN}{TP + TN + FP + FN} $$


#### Specificity
$$ Specificity=\dfrac{TN}{FP+T} $$


#### Kappa
$$ p_a =\dfrac{TP + TN}{TP + TN + FP + FN} $$

$$ p_e = \dfrac{(TN + FP) * (TN + FN) + (FN + TP) * (FP + TP)}{(TP + TN + FP + FN) * (TP + TN + FP + FN)} $$

$$ kappa = \dfrac{p_a - p_e}{1 - p_e} $$


#### Logloss
$$ logloss=- \dfrac{1}{N}\sum_{i=1}^N \sum_{j=1}^My_{i,j}log(p_{i,j}) $$


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String | ✓ | 所选列类型为 [STRING] |  |
| positiveLabelValueString | 正样本 | 正样本对应的字符串格式。 | String |  |  | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"],
    ["prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"],
    ["prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"],
    ["prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"],
    ["prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='label string, detailInput string')

metrics = EvalBinaryClassBatchOp().setLabelCol("label").setPredictionDetailCol("detailInput").linkFrom(inOp).collectMetrics()
print("AUC:", metrics.getAuc())
print("KS:", metrics.getKs())
print("PRC:", metrics.getPrc())
print("Accuracy:", metrics.getAccuracy())
print("Macro Precision:", metrics.getMacroPrecision())
print("Micro Recall:", metrics.getMicroRecall())
print("Weighted Sensitivity:", metrics.getWeightedSensitivity())
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.BinaryClassMetrics;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EvalBinaryClassBatchOpTest {
	@Test
	public void testEvalBinaryClassBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"),
			Row.of("prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"),
			Row.of("prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"),
			Row.of("prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
			Row.of("prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "label string, detailInput string");
		BinaryClassMetrics metrics = new EvalBinaryClassBatchOp().setLabelCol("label").setPredictionDetailCol(
			"detailInput").linkFrom(inOp).collectMetrics();
		System.out.println("AUC:" + metrics.getAuc());
		System.out.println("KS:" + metrics.getKs());
		System.out.println("PRC:" + metrics.getPrc());
		System.out.println("Accuracy:" + metrics.getAccuracy());
		System.out.println("Macro Precision:" + metrics.getMacroPrecision());
		System.out.println("Micro Recall:" + metrics.getMicroRecall());
		System.out.println("Weighted Sensitivity:" + metrics.getWeightedSensitivity());
	}
}
```

### 运行结果
```
AUC: 0.8333333333333334
KS: 0.6666666666666666
PRC: 0.9027777777777777
Accuracy: 0.6
Macro Precision: 0.8
Micro Recall: 0.6
Weighted Sensitivity: 0.6
```
