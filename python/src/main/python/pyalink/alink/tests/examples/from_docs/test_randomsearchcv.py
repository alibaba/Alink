import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestRandomSearchCV(unittest.TestCase):
    def test_randomsearchcv(self):

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
        pass