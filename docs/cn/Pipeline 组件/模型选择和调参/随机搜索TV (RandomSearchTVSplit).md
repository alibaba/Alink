# 随机搜索TV (RandomSearchTVSplit)
Java 类名：com.alibaba.alink.pipeline.tuning.RandomSearchTVSplit

Python 类名：RandomSearchTVSplit


## 功能介绍

randomsearch是通过随机参数，对其中的每一组输入参数的组很分别进行训练，预测，评估。取得评估参数最优的模型，作为最终的返回模型

tv为训练验证，将数据按照比例切分为两份，对其中一份数据做训练，对剩余一份数据做预测和评估，得到一个评估结果。

此函数用tv方法得到每一个grid对应参数的评估结果，得到最优模型

## 参数说明

| 名称            | 中文名称   | 描述                                         | 类型            | 是否必须？ | 默认值 |
| ---             | ---        | ---                                          | ---             | ---        | ---    |
| trainRatio      | 训练集比例 | 训练集与验证集的划分比例，取值范围为(0, 1]。 | Double          |             | 0.8    |
| ParamDist       | 参数分布   | 指定搜索的参数的分布                               | ParamDist       |     ✓       | ---    |
| Estimator       | Estimator  | 用于调优的Estimator                          | Estimator       | ✓           | ---    |
| TuningEvaluator | 评估指标   | 用于选择最优模型的评估指标                   | TuningEvaluator |       ✓     | ---    |

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
        .enableLazyPrintTrainInfo("TrainInfo")
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
        .enableLazyPrintTrainInfo("TrainInfo")
    )

    return cv


def tuningcv(cv_estimator, input):
    return cv_estimator.fit(input)


def tuningtv(tv_estimator, input):
    return tv_estimator.fit(input)


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
import com.alibaba.alink.pipeline.tuning.RandomSearchTVSplit;
import com.alibaba.alink.pipeline.tuning.RandomSearchTVSplitModel;
import com.alibaba.alink.pipeline.tuning.ValueDist;
import org.junit.Test;

public class RandomSearchTVSplitTest {
	@Test
	public void testRandomSearchTVSplit() throws Exception {
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
		RandomSearchTVSplit cv = new RandomSearchTVSplit()
			.setEstimator(rf)
			.setParamDist(paramDist)
			.setTuningEvaluator(tuningEvaluator)
			.setTrainRatio(0.8)
			.enableLazyPrintTrainInfo("TrainInfo");
		RandomSearchTVSplitModel model = cv.fit(data);
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
  |0.9121169398031198|RandomForestClassifier|numTrees|    8|
  |0.9105096451486404|RandomForestClassifier|numTrees|    7|
  |0.9105087086051442|RandomForestClassifier|numTrees|    6|
  |0.9098174499836453|RandomForestClassifier|numTrees|    6|
  |0.9089294943807537|RandomForestClassifier|numTrees|    4|
  |0.8910848199717841|RandomForestClassifier|numTrees|    2|
  |0.8862271106520978|RandomForestClassifier|numTrees|    2|
  |0.8748876808857913|RandomForestClassifier|numTrees|    2|
  | 0.858989501722944|RandomForestClassifier|numTrees|    1|
  |0.8553973913661752|RandomForestClassifier|numTrees|    1|



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
