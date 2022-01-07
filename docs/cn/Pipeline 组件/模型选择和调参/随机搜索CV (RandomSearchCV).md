# 随机搜索CV (RandomSearchCV)
Java 类名：com.alibaba.alink.pipeline.tuning.RandomSearchCV

Python 类名：RandomSearchCV


## 功能介绍

randomsearch是通过随机参数，对其中的每一组输入参数的组很分别进行训练，预测，评估。取得评估参数最优的模型，作为最终的返回模型

cv为交叉验证，将数据切分为k-folds，对每k-1份数据做训练，对剩余一份数据做预测和评估，得到一个评估结果。

此函数用cv方法得到每一个grid对应参数的评估结果，得到最优模型

## 参数说明

| 名称            | 中文名称  | 描述                                    | 类型            | 是否必须？ | 默认值 |
| ---             | ---       | ---                                     | ---             | ---        | ---    |
| NumFolds        | 折数      | 交叉验证的参数，数据的折数（大于等于2） | Integer         |            | 10     |
| ParamDist       | 参数分布  | 指定搜索的参数的分布                         | ParamDist       |  ✓          | ---    |
| Estimator       | Estimator | 用于调优的Estimator                     | Estimator       |  ✓          | ---    |
| TuningEvaluator | 评估指标  | 用于选择最优模型的评估指标              | TuningEvaluator |      ✓      | ---    |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

def adult(url):
    data = (
        CsvSourceBatchOp()
        .setFilePath('https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/adult_train.csv')
        .setSchemaStr(
            'age bigint, workclass string, fnlwgt bigint,'
            'education string, education_num bigint,'
            'marital_status string, occupation string,'
            'relationship string, race string, sex string,'
            'capital_gain bigint, capital_loss bigint,'
            'hours_per_week bigint, native_country string,'
            'label string'
        )
    )
    return data


def adult_train():
    return adult('https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/adult_train.csv')


def adult_test():
    return adult('https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/adult_test.csv')


def adult_numerical_feature_strs():
    return [
        "age", "fnlwgt", "education_num",
        "capital_gain", "capital_loss", "hours_per_week"
    ]


def adult_categorical_feature_strs():
    return [
        "workclass", "education", "marital_status",
        "occupation", "relationship", "race", "sex",
        "native_country"
    ]


def adult_features_strs():
    feature = adult_numerical_feature_strs()
    feature.extend(adult_categorical_feature_strs())

    return feature


def rf_grid_search_cv(featureCols, categoryFeatureCols, label, metric):
    rf = (
        RandomForestClassifier()
        .setFeatureCols(featureCols)
        .setCategoricalCols(categoryFeatureCols)
        .setLabelCol(label)
        .setPredictionCol('prediction')
        .setPredictionDetailCol('prediction_detail')
    )
    paramDist = (
        ParamDist()
        .addDist(rf, 'NUM_TREES', ValueDist.randInteger(1, 10))
    )
    tuningEvaluator = (
        BinaryClassificationTuningEvaluator()
        .setLabelCol(label)
        .setPredictionDetailCol("prediction_detail")
        .setTuningBinaryClassMetric(metric)
    )
    cv = (
        RandomSearchCV()
        .setEstimator(rf)
        .setParamDist(paramDist)
        .setTuningEvaluator(tuningEvaluator)
        .setNumFolds(2)
    )

    return cv


def rf_grid_search_tv(featureCols, categoryFeatureCols, label, metric):
    rf = (
        RandomForestClassifier()
        .setFeatureCols(featureCols)
        .setCategoricalCols(categoryFeatureCols)
        .setLabelCol(label)
        .setPredictionCol('prediction')
        .setPredictionDetailCol('prediction_detail')
    )
    paramDist = (
        ParamDist()
        .addDist(rf, 'NUM_TREES', ValueDist.randInteger(1, 10))
    )
    tuningEvaluator = (
        BinaryClassificationTuningEvaluator()
        .setLabelCol(label)
        .setPredictionDetailCol("prediction_detail")
        .setTuningBinaryClassMetric(metric)
    )
    cv = (
        RandomSearchTVSplit()
        .setEstimator(rf)
        .setParamDist(paramDist)
        .setTuningEvaluator(tuningEvaluator)
    )

    return cv


def tuningcv(cv_estimator, input):
    return cv_estimator.enableLazyPrintTrainInfo("CVTrainInfo").fit(input)


def tuningtv(tv_estimator, input):
    return tv_estimator.enableLazyPrintTrainInfo("TVTrainInfo").fit(input)


def main():
    print('rf cv tuning')
    model = tuningcv(
        rf_grid_search_cv(adult_features_strs(),
                          adult_categorical_feature_strs(), 'label', 'AUC'),
        adult_train()
    )

    print('rf tv tuning')
    model = tuningtv(
        rf_grid_search_tv(adult_features_strs(),
                          adult_categorical_feature_strs(), 'label', 'AUC'),
        adult_train()
    )

main()
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.classification.RandomForestClassifier;
import com.alibaba.alink.pipeline.tuning.BinaryClassificationTuningEvaluator;
import com.alibaba.alink.pipeline.tuning.ParamDist;
import com.alibaba.alink.pipeline.tuning.RandomSearchCV;
import com.alibaba.alink.pipeline.tuning.RandomSearchCVModel;
import com.alibaba.alink.pipeline.tuning.ValueDist;
import org.junit.Test;

public class RandomSearchCVTest {
	@Test
	public void testRandomSearchCV() throws Exception {
		String[] featureCols = new String[] {
			"age", "fnlwgt", "education_num",
			"capital_gain", "capital_loss", "hours_per_week",
			"workclass", "education", "marital_status",
			"occupation", "relationship", "race", "sex",
			"native_country"
		};
		String[] categoryFeatureCols = new String[] {
			"workclass", "education", "marital_status",
			"occupation", "relationship", "race", "sex",
			"native_country"
		};
		String label = "label";
		CsvSourceBatchOp data = new CsvSourceBatchOp()
			.setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/adult_train.csv")
			.setSchemaStr(
				"age bigint, workclass string, fnlwgt bigint, education string, education_num bigint, marital_status "
					+ "string, occupation string, relationship string, race string, sex string, capital_gain bigint, "
					+ "capital_loss bigint, hours_per_week bigint, native_country string, label string");
		RandomForestClassifier rf = new RandomForestClassifier()
			.setFeatureCols(featureCols)
			.setCategoricalCols(categoryFeatureCols)
			.setLabelCol(label)
			.setPredictionCol("prediction")
			.setPredictionDetailCol("prediction_detail");
		ParamDist paramDist = new ParamDist()
			.addDist(rf, RandomForestClassifier.NUM_TREES, ValueDist.randInteger(1, 10));
		BinaryClassificationTuningEvaluator tuningEvaluator = new BinaryClassificationTuningEvaluator()
			.setLabelCol(label)
			.setPredictionDetailCol("prediction_detail")
			.setTuningBinaryClassMetric("AUC");
		RandomSearchCV cv = new RandomSearchCV()
			.setEstimator(rf)
			.setParamDist(paramDist)
			.setTuningEvaluator(tuningEvaluator)
			.setNumFolds(2)
			.enableLazyPrintTrainInfo("TrainInfo");
		RandomSearchCVModel model = cv.fit(data);
	}
}
```

### 运行结果

TrainInfo
Metric information:
  Metric name: AUC
  Larger is better: true
Tuning information:
  |               AUC|                 stage|   param|value|
  |------------------|----------------------|--------|-----|
  |0.9134148020313496|RandomForestClassifier|numTrees|   10|
  |0.9123992401477525|RandomForestClassifier|numTrees|   10|
  |0.9107724678432794|RandomForestClassifier|numTrees|    8|
  | 0.905703319906151|RandomForestClassifier|numTrees|    6|
  |0.9052924036494705|RandomForestClassifier|numTrees|    7|
  |0.8927397325721704|RandomForestClassifier|numTrees|    3|
  |0.8887150253364192|RandomForestClassifier|numTrees|    3|
  | 0.885191174049819|RandomForestClassifier|numTrees|    3|
  |0.8837444737636566|RandomForestClassifier|numTrees|    2|
  |0.8774725529763574|RandomForestClassifier|numTrees|    2|



### 运行结果
```
rf cv tuning
com.alibaba.alink.pipeline.tuning.GridSearchCV
[ {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 3
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 1.0
  } ],
  "metric" : 0.8922549257899725
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 3
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.99
  } ],
  "metric" : 0.8920255970548456
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 3
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.98
  } ],
  "metric" : 0.8944982480437225
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 6
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 1.0
  } ],
  "metric" : 0.8923867598288401
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 6
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.99
  } ],
  "metric" : 0.9012141767959505
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 6
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.98
  } ],
  "metric" : 0.8993774036693788
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 9
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 1.0
  } ],
  "metric" : 0.8981738808130779
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 9
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.99
  } ],
  "metric" : 0.9029671873892725
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 9
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.98
  } ],
  "metric" : 0.905228896323363
} ]
rf tv tuning
com.alibaba.alink.pipeline.tuning.GridSearchTVSplit
[ {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 3
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 1.0
  } ],
  "metric" : 0.9022694229691741
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 3
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.99
  } ],
  "metric" : 0.8963559966080328
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 3
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.98
  } ],
  "metric" : 0.9041948454957178
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 6
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 1.0
  } ],
  "metric" : 0.8982021117392784
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 6
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.99
  } ],
  "metric" : 0.9031851535310546
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 6
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.98
  } ],
  "metric" : 0.9034443322241488
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 9
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 1.0
  } ],
  "metric" : 0.8993474753000145
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 9
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.99
  } ],
  "metric" : 0.9090250137144916
}, {
  "param" : [ {
    "stage" : "RandomForestClassifier",
    "paramName" : "numTrees",
    "paramValue" : 9
  }, {
    "stage" : "RandomForestClassifier",
    "paramName" : "subsamplingRatio",
    "paramValue" : 0.98
  } ],
  "metric" : 0.9129786771786127
} ]
```
