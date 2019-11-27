# gridsearchcv

## 功能介绍

gridsearch是通过参数数组组成的网格，对其中的每一组输入参数的组很分别进行训练，预测，评估。取得评估参数最优的模型，作为最终的返回模型

cv为交叉验证，将数据切分为k-folds，对每k-1份数据做训练，对剩余一份数据做预测和评估，得到一个评估结果。

此函数用cv方法得到每一个grid对应参数的评估结果，得到最优模型

## 参数说明

| 名称            | 中文名称  | 描述                                    | 类型            | 是否必须？ | 默认值 |
| ---             | ---       | ---                                     | ---             | ---        | ---    |
| NumFolds        | 折数      | 交叉验证的参数，数据的折数（大于等于2） | Integer         |            | 10     |
| ParamGrid       | 参数网格  | 指定参数的网格                          | ParamGrid       |     ✓       | ---    |
| Estimator       | Estimator | 用于调优的Estimator                     | Estimator       |     ✓       | ---    |
| TuningEvaluator | 评估指标  | 用于选择最优模型的评估指标              | TuningEvaluator |     ✓       | ---    |

## 脚本示例

#### 脚本代码

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
        .setMetricName(metric)
    )
    cv = (
        GridSearchCV()
        .setEstimator(rf)
        .setParamGrid(paramGrid)
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
    paramGrid = (
        ParamGrid()
        .addGrid(rf, 'SUBSAMPLING_RATIO', [1.0, 0.99, 0.98])
        .addGrid(rf, 'NUM_TREES', [3, 6, 9])
    )
    tuningEvaluator = (
        BinaryClassificationTuningEvaluator()
        .setLabelCol(label)
        .setPredictionDetailCol("prediction_detail")
        .setMetricName(metric)
    )
    cv = (
        GridSearchTVSplit()
        .setEstimator(rf)
        .setParamGrid(paramGrid)
        .setTuningEvaluator(tuningEvaluator)
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
    
    print(model.getReport())
    
    print('rf tv tuning')
    model = tuningtv(
        rf_grid_search_tv(adult_features_strs(),
                          adult_categorical_feature_strs(), 'label', 'AUC'),
        adult_train()
    )

    print(model.getReport())
main()
```

#### 脚本结果
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
