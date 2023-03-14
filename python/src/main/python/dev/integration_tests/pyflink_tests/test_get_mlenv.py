from pyalink.alink import *

mlenv = getMLEnv()
env, btenv, senv, stenv = mlenv

env.set_parallelism(2)
senv.set_parallelism(2)

source = CsvSourceBatchOp() \
    .setSchemaStr(
    "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
    .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
source.firstN(5).print()

t = stenv.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
source = TableSourceStreamOp(t)
source.print()
StreamOperator.execute()
