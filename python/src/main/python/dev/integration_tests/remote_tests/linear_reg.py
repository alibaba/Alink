import subprocess
import sys

import numpy as np
import pandas as pd

from pyalink.alink import *

if len(sys.argv) == 4:
    [_, host, port, local_ip] = sys.argv
else:
    host, port, local_ip = "localhost", 8081, "localhost"
print(host, port, local_ip)

useRemoteEnv(host, port, 2, shipAlinkAlgoJar=True, localIp=local_ip)

data = np.array([
    [2, 1, 1],
    [3, 2, 1],
    [4, 3, 2],
    [2, 4, 1],
    [2, 2, 1],
    [4, 3, 2],
    [1, 2, 1],
    [5, 3, 3]])

df = pd.DataFrame({"f0": data[:, 0],
                   "f1": data[:, 1],
                   "label": data[:, 2]})

batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
streamData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='stream')
colnames = ["f0", "f1"]
lr = LinearRegTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
model = batchData.link(lr)

predictor = LinearRegPredictStreamOp(model).setPredictionCol("pred")
predictor.linkFrom(streamData).print()
StreamOperator.execute()
