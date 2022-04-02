import subprocess
import sys

import pandas as pd

from pyalink.alink import *

if len(sys.argv) == 4:
    [_, host, port, local_ip] = sys.argv
else:
    host, port, local_ip = "localhost", 8081, "localhost"
print(host, port, local_ip)

useRemoteEnv(host, port, 2, shipAlinkAlgoJar=True)

source = (
    CsvSourceBatchOp()
        .setSchemaStr("id string,title string,content string")
        .setFilePath("http://alink-data.oss-cn-hangzhou-zmf.aliyuncs.com/data.csv")
        .setIgnoreFirstLine(True)
)

impterTrain = (
    ImputerTrainBatchOp()
        .setSelectedCols(['title', 'content'])
        .setStrategy('value')
        .setFillValue('')
)

impterPrediction = (
    ImputerPredictBatchOp()
        .setOutputCols(['title', 'content'])
)

segment = (
    SegmentBatchOp()
        .setSelectedCol('content')
)

stopWordRemover = (
    StopWordsRemoverBatchOp()
        .setSelectedCol('content')
)

wordvec = (
    Word2VecTrainBatchOp()
        .setSelectedCol('content')
        .setNumIter(1)
)

corpus = stopWordRemover.linkFrom(
    segment.linkFrom(
        impterPrediction.linkFrom(
            impterTrain.linkFrom(source), source
        )
    )
)

vec = wordvec.linkFrom(corpus)

words = BatchOperator.fromDataframe(
    pd.DataFrame([
        "武汉",
        "医院",
        "疫情",
        "口罩",
        "肺炎",
        "患者",
        "感染",
        "确诊",
        "隔离",
        "新冠",
        "物资",
        "病人",
        "医生",
        "病例",
        "病毒"
    ]),
    "words string"
)

showVec = (
    JoinBatchOp()
        .setJoinPredicate("a.word = b.words")
        .setSelectClause("a.*")
)

# sim = (
#     ApproxVectorSimilarityTopNLSHBatchOp()
#         .setLeftCol('vec')
#         .setRightCol('vec')
#         .setLeftIdCol('word')
#         .setRightIdCol('word')
#         .setDistanceType("EUCLIDEAN")
#         .setOutputCol("distance")
#         .setTopN(10)
# )

# similarTopN = collectToDataframes(
#     sim
#         .linkFrom(
#         vec,
#         showVec.linkFrom(vec, words)
#     )
# )
