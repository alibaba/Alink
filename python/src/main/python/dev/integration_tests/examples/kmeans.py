import numpy as np
import pandas as pd

from pyalink.alink import *

resetEnv()
useLocalEnv(1, config=None)

## prepare data
data = np.array([
    [0, 0.0, 0.0, 0.0],
    [1, 0.1, 0.1, 0.1],
    [2, 0.2, 0.2, 0.2],
    [3, 9, 9, 9],
    [4, 9.1, 9.1, 9.1],
    [5, 9.2, 9.2, 9.2]
])
df = pd.DataFrame({"id": data[:, 0], "f0": data[:, 1], "f1": data[:, 2], "f2": data[:, 3]})
inOp = BatchOperator.fromDataframe(df, schemaStr='id double, f0 double, f1 double, f2 double')
FEATURE_COLS = ["f0", "f1", "f2"]
VECTOR_COL = "vec"
PRED_COL = "pred"

vectorAssembler = (
    VectorAssembler()
        .setSelectedCols(FEATURE_COLS)
        .setOutputCol(VECTOR_COL)
)

kMeans = (
    KMeans()
        .setVectorCol(VECTOR_COL)
        .setK(2)
        .setPredictionCol(PRED_COL)
)

pipeline = Pipeline().add(vectorAssembler).add(kMeans)
pipeline.fit(inOp).transform(inOp).firstN(9).collectToDataframe()
