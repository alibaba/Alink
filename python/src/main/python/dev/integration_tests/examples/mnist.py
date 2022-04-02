from pyalink.alink import *

resetEnv()
useLocalEnv(1, config=None)

URL = "https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/mnist_dense.csv"
SCHEMA_STR = "label bigint,bitmap string"
mnist_data = CsvSourceBatchOp() \
    .setFilePath(URL) \
    .setSchemaStr(SCHEMA_STR) \
    .setFieldDelimiter(";")
spliter = SplitBatchOp().setFraction(0.8)
train = spliter.linkFrom(mnist_data)
test = spliter.getSideOutput(0)

softmax = Softmax().setVectorCol("bitmap").setLabelCol("label") \
    .setPredictionCol("pred").setPredictionDetailCol("detail") \
    .setEpsilon(0.0001).setMaxIter(200)
model = softmax.fit(train)
res = model.transform(test)

evaluation = EvalMultiClassBatchOp().setLabelCol("label").setPredictionCol("pred")
metrics = evaluation.linkFrom(res).collectMetrics()

print("ConfusionMatrix:", metrics.getConfusionMatrix())
print("LabelArray:", metrics.getLabelArray())
print("LogLoss:", metrics.getLogLoss())
print("TotalSamples:", metrics.getTotalSamples())
print("ActualLabelProportion:", metrics.getActualLabelProportion())
print("ActualLabelFrequency:", metrics.getActualLabelFrequency())
print("Accuracy:", metrics.getAccuracy())
print("Kappa:", metrics.getKappa())
