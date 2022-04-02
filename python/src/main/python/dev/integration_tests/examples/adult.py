from pyalink.alink import *

resetEnv()
useLocalEnv(1, config=None)

schema = "age bigint, workclass string, fnlwgt bigint, education string, \
          education_num bigint, marital_status string, occupation string, \
          relationship string, race string, sex string, capital_gain bigint, \
          capital_loss bigint, hours_per_week bigint, native_country string, label string"

adult_batch = CsvSourceBatchOp() \
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/adult_train.csv") \
    .setSchemaStr(schema)

adult_stream = CsvSourceStreamOp() \
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/adult_test.csv") \
    .setSchemaStr(schema)

categoricalColNames = ["workclass", "education", "marital_status", "occupation",
                       "relationship", "race", "sex", "native_country"]
numerialColNames = ["age", "fnlwgt", "education_num", "capital_gain",
                    "capital_loss", "hours_per_week"]
onehot = OneHotEncoder().setSelectedCols(categoricalColNames) \
    .setOutputCols(["output"]).setReservedCols(numerialColNames + ["label"])
assembler = VectorAssembler().setSelectedCols(["output"] + numerialColNames) \
    .setOutputCol("vec").setReservedCols(["label"])
pipeline = Pipeline().add(onehot).add(assembler)

logistic = LogisticRegression().setVectorCol("vec").setLabelCol("label") \
    .setPredictionCol("pred").setPredictionDetailCol("detail")
model = pipeline.add(logistic).fit(adult_batch)

predictBatch = model.transform(adult_batch)

metrics = EvalBinaryClassBatchOp().setLabelCol("label") \
    .setPredictionDetailCol("detail").linkFrom(predictBatch).collectMetrics()

print("AUC:", metrics.getAuc())
print("KS:", metrics.getKs())
print("PRC:", metrics.getPrc())
print("Precision:", metrics.getPrecision())
print("Recall:", metrics.getRecall())
print("F1:", metrics.getF1())
print("ConfusionMatrix:", metrics.getConfusionMatrix())
print("LabelArray:", metrics.getLabelArray())
print("LogLoss:", metrics.getLogLoss())
print("TotalSamples:", metrics.getTotalSamples())
print("ActualLabelProportion:", metrics.getActualLabelProportion())
print("ActualLabelFrequency:", metrics.getActualLabelFrequency())
print("Accuracy:", metrics.getAccuracy())
print("Kappa:", metrics.getKappa())
