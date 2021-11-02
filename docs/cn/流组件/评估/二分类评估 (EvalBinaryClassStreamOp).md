# 二分类评估 (EvalBinaryClassStreamOp)
Java 类名：com.alibaba.alink.operator.stream.evaluation.EvalBinaryClassStreamOp

Python 类名：EvalBinaryClassStreamOp


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
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String | ✓ |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| positiveLabelValueString | 正样本 | 正样本对应的字符串格式。 | String |  | null |
| timeInterval | 时间间隔 | 流式数据统计的时间间隔 | Double |  | 3.0 |



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
