import sys

from pyalink.alink import *

if len(sys.argv) == 4:
    [_, host, port, local_ip] = sys.argv
else:
    host, port, local_ip = "localhost", 8081, "localhost"
print(host, port, local_ip)

useRemoteEnv(host, port, 2, shipAlinkAlgoJar=True)
source = CsvSourceBatchOp() \
    .setSchemaStr(
    "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
    .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
source.firstN(5).print()

useRemoteEnv(host, port, 2, shipAlinkAlgoJar=True, localIp=local_ip)
source = CsvSourceStreamOp() \
    .setSchemaStr(
    "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
    .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
source.print()
StreamOperator.execute()

useRemoteEnv(host, port, 2, shipAlinkAlgoJar=True)
benv, btenv, senv, stenv = getMLEnv()
t = btenv.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
source = TableSourceBatchOp(t)
source.print()
