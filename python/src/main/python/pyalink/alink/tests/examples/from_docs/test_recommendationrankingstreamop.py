import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestRecommendationRankingStreamOp(unittest.TestCase):
    def test_recommendationrankingstreamop(self):

        import pandas as pd
        
        data = pd.DataFrame([["u6", "0.0 1.0", 0.0, 1.0, 1, "{\"data\":{\"iid\":[18,19,88]},\"schema\":\"iid INT\"}"]])
        predData = StreamOperator.fromDataframe(data, schemaStr='uid string, uf string, f0 double, f1 double, labels int, ilist string')
        predData = predData.link(ToMTableStreamOp().setSelectedCol("ilist"))
        data = pd.DataFrame([
                    ["u0", "1.0 1.0", 1.0, 1.0, 1, 18],
        			["u1", "1.0 1.0", 1.0, 1.0, 0, 19],
        			["u2", "1.0 0.0", 1.0, 0.0, 1, 88],
        			["u3", "1.0 0.0", 1.0, 0.0, 0, 18],
        			["u4", "0.0 1.0", 0.0, 1.0, 1, 88],
        			["u5", "0.0 1.0", 0.0, 1.0, 0, 19],
        			["u6", "0.0 1.0", 0.0, 1.0, 1, 88]]);
        trainData = BatchOperator.fromDataframe(data, schemaStr='uid string, uf string, f0 double, f1 double, labels int, iid string')
        oneHotCols = ["uid", "f0", "f1", "iid"]
        multiHotCols = ["uf"]
        pipe = Pipeline() \
            .add( \
                OneHotEncoder() \
                    .setSelectedCols(oneHotCols) \
        			.setOutputCols(["ovec"])) \
            .add( \
        		MultiHotEncoder().setDelimiter(" ") \
                    .setSelectedCols(multiHotCols) \
                    .setOutputCols(["mvec"])) \
        	.add( \
        		VectorAssembler() \
        			.setSelectedCols(["ovec", "mvec"]) \
        			.setOutputCol("vec")) \
        	.add(
        		LogisticRegression() \
                    .setVectorCol("vec") \
        			.setLabelCol("labels") \
        			.setReservedCols(["uid", "iid"]) \
        			.setPredictionDetailCol("detail") \
        			.setPredictionCol("pred")) \
        	.add( \
        		JsonValue() \
        			.setSelectedCol("detail") \
        			.setJsonPath(["$.1"]) \
        			.setOutputCols(["score"]))
        lrModel = pipe.fit(trainData)
        rank = RecommendationRankingStreamOp(lrModel.save())\
        			.setMTableCol("ilist")\
        			.setOutputCol("il")\
        			.setTopN(2)\
        			.setRankingCol("score")\
        			.setReservedCols(["uid", "labels"])
        rank.linkFrom(predData).print()
        StreamOperator.execute()
        pass