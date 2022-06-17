# 二分类评估 (EvalBinaryClassBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalBinaryClassBatchOp

Python 类名：EvalBinaryClassBatchOp


## 功能介绍

对二分类算法的预测结果进行效果评估。

### 算法原理

在有监督二分类问题的评估中，每条样本都有一个真实的标签和一个由模型生成的预测。 这样每调样本点实际上可以划分为以下 4 个类别中的一类：

 - True Positive（TP）：标签为正类，预测为正类；
 - True Negative（TN）：标签为负类，预测为负类；
 - False Positive（FP）：标签为负类，预测为正类；
 - False Negative（FN）：标签为正类，预测为负类。

通常，用 $TP, TN, FP, FN$ 分别表示属于各自类别的样本数。 
基于这几个数量，可以定义大部分的二分类评估指标。

#### 精确率

$Precision = \frac{TP}{TP + FP}$

#### 召回率、敏感性

$Recall = \frac{TP}{TP + FN} = Sensitivity$

#### F-measure

$F1=\frac{2TP}{2TP+FP+FN}=\frac{2\cdot Precision \cdot Recall}{Precision+Recall}$

#### 准确率

$Accuracy=\frac{TP + TN}{TP + TN + FP + FN}$

#### 特异性

$Specificity=\frac{TN}{FP+TN}$

#### Kappa

$p_a =\frac{TP + TN}{TP + TN + FP + FN}$

$p_e = \frac{(TN + FP) * (TN + FN) + (FN + TP) * (FP + TP)}{(TP + TN + FP + FN) * (TP + TN + FP + FN)}$

$kappa = \frac{p_a - p_e}{1 - p_e}$

#### 混淆矩阵

<div align=center><img src="https://img.alicdn.com/tfs/TB1lCMOaW67gK0jSZFHXXa9jVXa-1582-688.jpg" height="50%" width="50%"></div>


二分类模型除了给出每条样本的预测标签之外，通常还会给出每条样本预测为正类的概率$p$，而预测标签是根据这个概率与阈值的关系确定的。
通常情况下，阈值会设为 0.5，概率大于 0.5 的预测为正类，小于 0.5 的预测为负类。
有一些评估指标会考虑这个阈值从 0 到 1 变化时，各个指标的变化情况，计算更加复杂的指标。

#### LogLoss

$LogLoss = -\frac{1}{n}\sum_{i}[y_i \log(y'_i) + (1-y_i) \log(1 - y'_i)]$

这里，$y_i\in [0,1]$表示样本$i$的真实标签（正类为 1，负类为0），$y'_i$表示样本$i$预测为正类的概率。

#### ROC（receiver operating characteristic）曲线

阈值从 0 到 1 变化时， 横坐标：$FPR = FP / (FP + TN)$ 和纵坐标：$TPR = TP / (TP + FN)$构成的曲线。

#### AUC (Area under curve)

ROC 曲线下的面积。

#### K-S 曲线

阈值从 0 到 1 变化时，横坐标阈值和纵坐标$TPR$和$FPR$构成的曲线。

#### KS 指标

K-S 曲线 中，两条曲线在纵轴方向上的最大差值。

#### Precision-Recall 曲线

阈值从 0 到 1 变化时，横坐标 Precision 和纵坐标 Recall 构成的曲线。

#### PRC 指标

Precision-Recall 曲线下的面积。

#### 提升曲线（Lift Chart）

阈值从 0 到 1 变化时，横坐标$\frac{TP + FP}{N}$和纵坐标$TP$构成的曲线。

### 使用方式

该组件通常接二分类预测算法的输出端。

使用时，需要通过参数 `labelCol` 指定预测标签列，通过参数 `predictionDetailCol` 指定预测详细信息列（包含有预测概率）。


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
