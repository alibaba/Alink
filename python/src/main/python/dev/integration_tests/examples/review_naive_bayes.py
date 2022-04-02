# set env
from pyalink.alink import *

resetEnv()
useLocalEnv(1, config=None)

## read data
URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/review_rating_train.csv"
SCHEMA_STR = "review_id bigint, rating5 bigint, rating3 bigint, review_context string"
LABEL_COL = "rating5"
TEXT_COL = "review_context"
VECTOR_COL = "vec"
PRED_COL = "pred"
PRED_DETAIL_COL = "predDetail"
source = CsvSourceBatchOp() \
    .setFilePath(URL) \
    .setSchemaStr(SCHEMA_STR) \
    .setFieldDelimiter("_alink_") \
    .setQuoteChar(None)

## Split data for train and test
trainData = SplitBatchOp().setFraction(0.9).linkFrom(source)
testData = trainData.getSideOutput(0)

pipeline = (
    Pipeline()
        .add(
        Segment()
            .setSelectedCol(TEXT_COL)
    )
        .add(
        StopWordsRemover()
            .setSelectedCol(TEXT_COL)
    ).add(
        DocHashCountVectorizer()
            .setFeatureType("WORD_COUNT")
            .setSelectedCol(TEXT_COL)
            .setOutputCol(VECTOR_COL)
    )
)

## naiveBayes model
naiveBayes = (
    NaiveBayesTextClassifier()
        .setVectorCol(VECTOR_COL)
        .setLabelCol(LABEL_COL)
        .setPredictionCol(PRED_COL)
        .setPredictionDetailCol(PRED_DETAIL_COL)
)
model = pipeline.add(naiveBayes).fit(trainData)

## evaluation
predict = model.transform(testData)
metrics = (
    EvalMultiClassBatchOp()
        .setLabelCol(LABEL_COL)
        .setPredictionDetailCol(PRED_DETAIL_COL)
        .linkFrom(predict)
        .collectMetrics()
)

print("ConfusionMatrix:", metrics.getConfusionMatrix())
print("LabelArray:", metrics.getLabelArray())
print("LogLoss:", metrics.getLogLoss())
print("Accuracy:", metrics.getAccuracy())
print("Kappa:", metrics.getKappa())
print("MacroF1:", metrics.getMacroF1())
print("Label 1 Accuracy:", metrics.getAccuracy("1"))
print("Label 1 Kappa:", metrics.getKappa("1"))
print("Label 1 Precision:", metrics.getPrecision("1"))
