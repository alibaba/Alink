from pyalink.alink import *

mlenv = getMLEnv()
source = CsvSourceBatchOp() \
    .setSchemaStr(
    "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
    .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
source.firstN(5).print()

env, btenv, senv, stenv = mlenv
t = stenv.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
source = TableSourceStreamOp(t)
source.print()
StreamOperator.execute()
