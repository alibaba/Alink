# 二分类评估 (EvalBinaryClassStreamOp)
Java 类名：com.alibaba.alink.operator.stream.evaluation.EvalBinaryClassStreamOp

Python 类名：EvalBinaryClassStreamOp


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
另外，需要指定参数 `timeInterval`，表示对数据按时间窗口来进行划分。
在输出结果中既包含各个时间窗口内的统计指标，也包含此前所有数据的统计指标。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String | ✓ | 所选列类型为 [STRING] |  |
| positiveLabelValueString | 正样本 | 正样本对应的字符串格式。 | String |  |  | null |
| timeInterval | 时间间隔 | 流式数据统计的时间间隔 | Double |  |  | 3.0 |

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

inOp = StreamOperator.fromDataframe(df, schemaStr='label string, detailInput string')

EvalBinaryClassStreamOp().setLabelCol("label").setPredictionDetailCol("detailInput").setTimeInterval(0.001).linkFrom(inOp).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.evaluation.EvalBinaryClassStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EvalBinaryClassStreamOpTest {
	@Test
	public void testEvalBinaryClassStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"),
			Row.of("prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"),
			Row.of("prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"),
			Row.of("prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
			Row.of("prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}")
		);
		StreamOperator <?> inOp = new MemSourceStreamOp(df, "label string, detailInput string");
		new EvalBinaryClassStreamOp().setLabelCol("label").setPredictionDetailCol("detailInput").setTimeInterval(0.001)
			.linkFrom(inOp).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

Statistics|Data
----------|----
window|{"PRC":"1.0","SensitivityArray":"[1.0,1.0]","ConfusionMatrix":"[[1,0],[0,0]]","MacroRecall":"0.5","MacroSpecificity":"0.5","FalsePositiveRateArray":"[0.0,0.0]","TruePositiveRateArray":"[1.0,1.0]","AUC":"NaN","MacroAccuracy":"1.0","RecallArray":"[1.0,1.0]","KappaArray":"[1.0,1.0]","MicroFalseNegativeRate":"0.0","WeightedRecall":"1.0","WeightedPrecision":"1.0","Recall":"1.0","MacroPrecision":"1.0","ThresholdArray":"[0.8,0.5]","ActualLabelFrequency":"[1,0]","PrecisionArray":"[1.0,1.0]","MicroTruePositiveRate":"1.0","RocCurve":"[[0.0,1.0,1.0],[0.0,1.0,1.0]]","MacroKappa":"1.0","MicroSpecificity":"1.0","F1":"1.0","F1Array":"[1.0,1.0]","Precision":"1.0","MacroFalsePositiveRate":"0.0","FalseNegativeRateArray":"[0.0,0.0]","TrueNegativeRateArray":"[0.0,0.0]","MacroF1":"0.5","GINI":"0.0","LorenzCurve":"[[0.0,1.0,1.0],[0.0,1.0,1.0]]","LabelArray":"[\"prefix1\",\"prefix0\"]","WeightedTruePositiveRate":"1.0","WeightedKappa":"1.0","TotalSamples":"1","MicroTrueNegativeRate":"1.0","LiftChart":"[[0.0,1.0,1.0],[0.0,1.0,1.0]]","MacroTruePositiveRate":"0.5","MicroSensitivity":"1.0","WeightedAccuracy":"1.0","K-S":"0.0","AccuracyArray":"[1.0,1.0]","Accuracy":"1.0","WeightedFalseNegativeRate":"0.0","MicroF1":"1.0","WeightedSpecificity":"0.0","WeightedF1":"1.0","MicroAccuracy":"1.0","WeightedTrueNegativeRate":"0.0","Kappa":"1.0","MacroSensitivity":"0.5","SpecificityArray":"[0.0,0.0]","ActualLabelProportion":"[1.0,0.0]","RecallPrecisionCurve":"[[0.0,1.0,1.0],[1.0,1.0,1.0]]","WeightedSensitivity":"1.0","MicroRecall":"1.0","MacroFalseNegativeRate":"0.0","LogLoss":"0.2231435513142097","MicroFalsePositiveRate":"0.0","WeightedFalsePositiveRate":"0.0","MacroTrueNegativeRate":"0.5","MicroPrecision":"1.0","MicroKappa":"1.0"}
all|{"PRC":"1.0","SensitivityArray":"[1.0,1.0]","ConfusionMatrix":"[[1,0],[0,0]]","MacroRecall":"0.5","MacroSpecificity":"0.5","FalsePositiveRateArray":"[0.0,0.0]","TruePositiveRateArray":"[1.0,1.0]","AUC":"NaN","MacroAccuracy":"1.0","RecallArray":"[1.0,1.0]","KappaArray":"[1.0,1.0]","MicroFalseNegativeRate":"0.0","WeightedRecall":"1.0","WeightedPrecision":"1.0","Recall":"1.0","MacroPrecision":"1.0","ThresholdArray":"[0.8,0.5]","ActualLabelFrequency":"[1,0]","PrecisionArray":"[1.0,1.0]","MicroTruePositiveRate":"1.0","RocCurve":"[[0.0,1.0,1.0],[0.0,1.0,1.0]]","MacroKappa":"1.0","MicroSpecificity":"1.0","F1":"1.0","F1Array":"[1.0,1.0]","Precision":"1.0","MacroFalsePositiveRate":"0.0","FalseNegativeRateArray":"[0.0,0.0]","TrueNegativeRateArray":"[0.0,0.0]","MacroF1":"0.5","GINI":"0.0","LorenzCurve":"[[0.0,1.0,1.0],[0.0,1.0,1.0]]","LabelArray":"[\"prefix1\",\"prefix0\"]","WeightedTruePositiveRate":"1.0","WeightedKappa":"1.0","TotalSamples":"1","MicroTrueNegativeRate":"1.0","LiftChart":"[[0.0,1.0,1.0],[0.0,1.0,1.0]]","MacroTruePositiveRate":"0.5","MicroSensitivity":"1.0","WeightedAccuracy":"1.0","K-S":"0.0","AccuracyArray":"[1.0,1.0]","Accuracy":"1.0","WeightedFalseNegativeRate":"0.0","MicroF1":"1.0","WeightedSpecificity":"0.0","WeightedF1":"1.0","MicroAccuracy":"1.0","WeightedTrueNegativeRate":"0.0","Kappa":"1.0","MacroSensitivity":"0.5","SpecificityArray":"[0.0,0.0]","ActualLabelProportion":"[1.0,0.0]","RecallPrecisionCurve":"[[0.0,1.0,1.0],[1.0,1.0,1.0]]","WeightedSensitivity":"1.0","MicroRecall":"1.0","MacroFalseNegativeRate":"0.0","LogLoss":"0.2231435513142097","MicroFalsePositiveRate":"0.0","WeightedFalsePositiveRate":"0.0","MacroTrueNegativeRate":"0.5","MicroPrecision":"1.0","MicroKappa":"1.0"}
