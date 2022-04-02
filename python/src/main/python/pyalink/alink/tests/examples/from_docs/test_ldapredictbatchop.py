import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLdaPredictBatchOp(unittest.TestCase):
    def test_ldapredictbatchop(self):

        df = pd.DataFrame([
            ["a b b c c c c c c e e f f f g h k k k"], 
            ["a b b b d e e e h h k"], 
            ["a b b b b c f f f f g g g g g g g g g i j j"], 
            ["a a b d d d g g g g g i i j j j k k k k k k k k k"], 
            ["a a a b c d d d d d d d d d e e e g g j k k k"], 
            ["a a a a b b d d d e e e e f f f f f g h i j j j j"], 
            ["a a b d d d g g g g g i i j j k k k k k k k k k"], 
            ["a b c d d d d d d d d d e e f g g j k k k"], 
            ["a a a a b b b b d d d e e e e f f g h h h"], 
            ["a a b b b b b b b b c c e e e g g i i j j j j j j j k k"], 
            ["a b c d d d d d d d d d f f g g j j j k k k"], 
            ["a a a a b e e e e f f f f f g h h h j"],
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr="doc string")
        inOp2 = StreamOperator.fromDataframe(df, schemaStr="doc string")
        
        ldaTrain = LdaTrainBatchOp()\
                    .setSelectedCol("doc")\
                    .setTopicNum(6)\
                    .setMethod("online")\
                    .setSubsamplingRate(1.0)\
                    .setOptimizeDocConcentration(True)\
                    .setNumIter(20)
        
        ldaPredict = LdaPredictBatchOp()\
            .setPredictionCol("pred")\
            .setSelectedCol("doc")
        
        model = ldaTrain.linkFrom(inOp)
        ldaPredict.linkFrom(model, inOp)
        
        model.lazyPrint(10)
        ldaPredict.print()
        
        ldaPredictS = LdaPredictStreamOp(model)\
            .setPredictionCol("pred")\
            .setSelectedCol("doc")\
            .linkFrom(inOp2)
        
        ldaPredictS.print()
        
        StreamOperator.execute()
        pass