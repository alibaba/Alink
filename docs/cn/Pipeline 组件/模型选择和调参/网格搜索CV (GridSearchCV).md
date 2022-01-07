# 网格搜索CV (GridSearchCV)
Java 类名：com.alibaba.alink.pipeline.tuning.GridSearchCV

Python 类名：GridSearchCV


## 功能介绍

gridsearch是通过参数数组组成的网格，对其中的每一组输入参数的组很分别进行训练，预测，评估。取得评估参数最优的模型，作为最终的返回模型

cv为交叉验证，将数据切分为k-folds，对每k-1份数据做训练，对剩余一份数据做预测和评估，得到一个评估结果。

此函数用cv方法得到每一个grid对应参数的评估结果，得到最优模型

## 参数说明

| 名称            | 中文名称  | 描述                                    | 类型            | 是否必须？ | 默认值 |
| ---             | ---       | ---                                     | ---             | ---        | ---    |
| NumFolds        | 折数      | 交叉验证的参数，数据的折数（大于等于2） | Integer         |            | 10     |
| ParamGrid       | 参数网格  | 指定参数的网格                          | ParamGrid       |  ✓          | ---    |
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
    paramGrid = (
        ParamGrid()
        .addGrid(rf, 'SUBSAMPLING_RATIO', [1.0, 0.99, 0.98])
        .addGrid(rf, 'NUM_TREES', [3, 6, 9])
    )
    tuningEvaluator = (
        BinaryClassificationTuningEvaluator()
        .setLabelCol(label)
        .setPredictionDetailCol("prediction_detail")
        .setTuningBinaryClassMetric(metric)
    )
    cv = (
        GridSearchCV()
        .setEstimator(rf)
        .setParamGrid(paramGrid)
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
    paramGrid = (
        ParamGrid()
        .addGrid(rf, 'SUBSAMPLING_RATIO', [1.0, 0.99, 0.98])
        .addGrid(rf, 'NUM_TREES', [3, 6, 9])
    )
    tuningEvaluator = (
        BinaryClassificationTuningEvaluator()
        .setLabelCol(label)
        .setPredictionDetailCol("prediction_detail")
        .setTuningBinaryClassMetric(metric)
    )
    cv = (
        GridSearchTVSplit()
        .setEstimator(rf)
        .setParamGrid(paramGrid)
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
import com.alibaba.alink.pipeline.tuning.GridSearchCV;
import com.alibaba.alink.pipeline.tuning.GridSearchCVModel;
import com.alibaba.alink.pipeline.tuning.ParamGrid;
import org.junit.Test;

public class GridSearchCVTest {
	@Test
	public void testGridSearchCV() throws Exception {
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
		ParamGrid paramGrid = new ParamGrid()
			.addGrid(rf, RandomForestClassifier.SUBSAMPLING_RATIO, new Double[] {1.0, 0.99, 0.98})
			.addGrid(rf, RandomForestClassifier.NUM_TREES, new Integer[] {3, 6, 9});
		BinaryClassificationTuningEvaluator tuningEvaluator = new BinaryClassificationTuningEvaluator()
			.setLabelCol(label)
			.setPredictionDetailCol("prediction_detail")
			.setTuningBinaryClassMetric("AUC");
		GridSearchCV cv = new GridSearchCV()
			.setEstimator(rf)
			.setParamGrid(paramGrid)
			.setTuningEvaluator(tuningEvaluator)
			.setNumFolds(2)
			.enableLazyPrintTrainInfo("TrainInfo");
		GridSearchCVModel model = cv.fit(data);
	}
}
```

### 运行结果

TrainInfo
Metric information:
  Metric name: AUC
  Larger is better: true
Tuning information:
  |               AUC|                 stage|   param|value|               stage 2|         param 2|value 2|
  |------------------|----------------------|--------|-----|----------------------|----------------|-------|
  | 0.912327454540025|RandomForestClassifier|numTrees|    9|RandomForestClassifier|subsamplingRatio|   0.98|
  |0.9113181022628927|RandomForestClassifier|numTrees|    9|RandomForestClassifier|subsamplingRatio|    1.0|
  |0.9109408773009041|RandomForestClassifier|numTrees|    9|RandomForestClassifier|subsamplingRatio|   0.99|
  |0.9084745064874684|RandomForestClassifier|numTrees|    6|RandomForestClassifier|subsamplingRatio|    1.0|
  |0.9066321684664669|RandomForestClassifier|numTrees|    6|RandomForestClassifier|subsamplingRatio|   0.98|
  |0.9045123178682739|RandomForestClassifier|numTrees|    6|RandomForestClassifier|subsamplingRatio|   0.99|
  |0.8908957160768797|RandomForestClassifier|numTrees|    3|RandomForestClassifier|subsamplingRatio|    1.0|
  |0.8903604608878586|RandomForestClassifier|numTrees|    3|RandomForestClassifier|subsamplingRatio|   0.98|
  | 0.888885807369439|RandomForestClassifier|numTrees|    3|RandomForestClassifier|subsamplingRatio|   0.99|


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
