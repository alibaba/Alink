## Description
Grid search implemented by Cross validation.

 Grid search is an approach to parameter tuning that will methodically build and evaluate a model for each combination
 of algorithm parameters specified in a grid.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| NumFolds | Number of folds for cross validation (>= 2) | Integer |  | 10 |
| lazyPrintTrainInfoEnabled | Enable lazyPrint of TrainInfo | Boolean |  | false |
| lazyPrintTrainInfoTitle | Title of TrainInfo in lazyPrint | String |  | null |

## Script Example

#### Code

```python
def adult(url):
    data = (
        CsvSourceBatchOp()
        .setFilePath('http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/adult_train.csv')
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
    return adult('http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/adult_train.csv')


def adult_test():
    return adult('http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/adult_test.csv')


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

#### Result
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
