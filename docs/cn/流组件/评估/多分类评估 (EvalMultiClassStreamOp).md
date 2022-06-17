# 多分类评估 (EvalMultiClassStreamOp)
Java 类名：com.alibaba.alink.operator.stream.evaluation.EvalMultiClassStreamOp

Python 类名：EvalMultiClassStreamOp


## 功能介绍

对多分类算法的预测结果进行效果评估。

### 算法原理

在多分类问题的评估中，每条样本都有一个真实的标签和一个由模型生成的预测。
但与二分类问题不同，多分类算法中，总的类别数是大于2的，因此不能直接称作正类和负类。

在计算评估指标时，可以将某个类别选定为正类，将其他值都看作负类，这样可以计算每个类别（per-class）的指标。
进一步地，将每个类别各自的指标进行平均，可以得到模型总体的指标。
这里的"平均"有三种做法：

- Macro 平均：直接对各个类别的同一个指标求数值平均值，作为总体指标；
- 加权平均：以样本中各个类别所占的比例为权重，对各个类别的同一个指标求加权平均值，作为总体指标；
- Micro 平均：将各个类别看作正类时的 $TP, TN, FN$ 相加，得到总的 $TP, TN, FN$ 值，然后计算指标。在 Micro 平均时，micro-F1, micro-precision, micro-recall 都等于 accuracy。

所支持的每类别指标与平均指标见下：

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

二分类模型除了给出每条样本$i$的预测标签之外，通常还会给出每条样本预测为为各个类别$j$的概率$p_{i,j}$。
通常情况下，每条样本最大概率对应的类别为该样本的预测标签。

#### LogLoss

$LogLoss=- \frac{1}{n}\sum_{i} \sum_{j=1}^M y_{i,j}log(p_{i,j})$

### 使用方式

该组件通常接多分类预测算法的输出端。

使用时，需要通过参数 `labelCol` 指定预测标签列，通过参数 `predictionCol` 和 `predictionDetailCol` 指定预测结果列和预测详细信息列（包含有预测概率）。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
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

EvalMultiClassStreamOp().setLabelCol("label").setPredictionDetailCol("detailInput").setTimeInterval(0.001).linkFrom(inOp).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.evaluation.EvalMultiClassStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EvalMultiClassStreamOpTest {
	@Test
	public void testEvalMultiClassStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"),
			Row.of("prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"),
			Row.of("prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"),
			Row.of("prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"),
			Row.of("prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}")
		);
		StreamOperator <?> inOp = new MemSourceStreamOp(df, "label string, detailInput string");
		new EvalMultiClassStreamOp().setLabelCol("label").setPredictionDetailCol("detailInput").setTimeInterval(0.001)
			.linkFrom(inOp).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

Statistics|Data
----------|----
all|{"SensitivityArray":"[1.0,0.0,1.0,0.5,1.0]","ConfusionMatrix":"[[1,0],[0,0]]","MacroRecall":"0.5","MacroSpecificity":"0.5","FalsePositiveRateArray":"[0.0,0.0,0.0,0.0,0.0]","TruePositiveRateArray":"[1.0,0.0,1.0,0.5,1.0]","MacroAccuracy":"1.0","RecallArray":"[1.0,0.0,1.0,0.5,1.0]","KappaArray":"[1.0,1.0,1.0,1.0,1.0]","MicroFalseNegativeRate":"0.0","WeightedRecall":"1.0","WeightedPrecision":"1.0","MacroPrecision":"1.0","ActualLabelFrequency":"[1,0]","PrecisionArray":"[1.0,1.0,1.0,1.0,1.0]","MicroTruePositiveRate":"1.0","MacroKappa":"1.0","MicroSpecificity":"1.0","PredictLabelProportion":"[1.0,0.0]","F1Array":"[1.0,0.0,1.0,0.5,1.0]","MacroFalsePositiveRate":"0.0","FalseNegativeRateArray":"[0.0,0.0,0.0,0.0,0.0]","TrueNegativeRateArray":"[0.0,1.0,0.0,0.5,1.0]","MacroF1":"0.5","LabelArray":"[\"prefix1\",\"prefix0\"]","WeightedTruePositiveRate":"1.0","WeightedKappa":"1.0","TotalSamples":"1","MicroTrueNegativeRate":"1.0","MacroTruePositiveRate":"0.5","MicroSensitivity":"1.0","WeightedAccuracy":"1.0","AccuracyArray":"[1.0,1.0,1.0,1.0,1.0]","Accuracy":"1.0","WeightedFalseNegativeRate":"0.0","MicroF1":"1.0","WeightedSpecificity":"0.0","WeightedF1":"1.0","MicroAccuracy":"1.0","WeightedTrueNegativeRate":"0.0","Kappa":"1.0","MacroSensitivity":"0.5","SpecificityArray":"[0.0,1.0,0.0,0.5,1.0]","ActualLabelProportion":"[1.0,0.0]","PredictLabelFrequency":"[1,0]","WeightedSensitivity":"1.0","MicroRecall":"1.0","MacroFalseNegativeRate":"0.0","LogLoss":"0.35667494393873245","MicroFalsePositiveRate":"0.0","WeightedFalsePositiveRate":"0.0","MacroTrueNegativeRate":"0.5","MicroPrecision":"1.0","MicroKappa":"1.0"}
window|{"SensitivityArray":"[1.0,0.0,1.0,0.5,1.0]","ConfusionMatrix":"[[1,0],[0,0]]","MacroRecall":"0.5","MacroSpecificity":"0.5","FalsePositiveRateArray":"[0.0,0.0,0.0,0.0,0.0]","TruePositiveRateArray":"[1.0,0.0,1.0,0.5,1.0]","MacroAccuracy":"1.0","RecallArray":"[1.0,0.0,1.0,0.5,1.0]","KappaArray":"[1.0,1.0,1.0,1.0,1.0]","MicroFalseNegativeRate":"0.0","WeightedRecall":"1.0","WeightedPrecision":"1.0","MacroPrecision":"1.0","ActualLabelFrequency":"[1,0]","PrecisionArray":"[1.0,1.0,1.0,1.0,1.0]","MicroTruePositiveRate":"1.0","MacroKappa":"1.0","MicroSpecificity":"1.0","PredictLabelProportion":"[1.0,0.0]","F1Array":"[1.0,0.0,1.0,0.5,1.0]","MacroFalsePositiveRate":"0.0","FalseNegativeRateArray":"[0.0,0.0,0.0,0.0,0.0]","TrueNegativeRateArray":"[0.0,1.0,0.0,0.5,1.0]","MacroF1":"0.5","LabelArray":"[\"prefix1\",\"prefix0\"]","WeightedTruePositiveRate":"1.0","WeightedKappa":"1.0","TotalSamples":"1","MicroTrueNegativeRate":"1.0","MacroTruePositiveRate":"0.5","MicroSensitivity":"1.0","WeightedAccuracy":"1.0","AccuracyArray":"[1.0,1.0,1.0,1.0,1.0]","Accuracy":"1.0","WeightedFalseNegativeRate":"0.0","MicroF1":"1.0","WeightedSpecificity":"0.0","WeightedF1":"1.0","MicroAccuracy":"1.0","WeightedTrueNegativeRate":"0.0","Kappa":"1.0","MacroSensitivity":"0.5","SpecificityArray":"[0.0,1.0,0.0,0.5,1.0]","ActualLabelProportion":"[1.0,0.0]","PredictLabelFrequency":"[1,0]","WeightedSensitivity":"1.0","MicroRecall":"1.0","MacroFalseNegativeRate":"0.0","LogLoss":"0.35667494393873245","MicroFalsePositiveRate":"0.0","WeightedFalsePositiveRate":"0.0","MacroTrueNegativeRate":"0.5","MicroPrecision":"1.0","MicroKappa":"1.0"}
