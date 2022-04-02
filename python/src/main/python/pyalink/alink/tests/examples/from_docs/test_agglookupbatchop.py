import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAggLookupBatchOp(unittest.TestCase):
    def test_agglookupbatchop(self):

        df = pd.DataFrame([
            ["the quality of the word vectors increases"],
            ["amount of the training data increases"],
            ["the training speed is significantly improved"]
        ])
        
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='sentence string')
        
        df2 = pd.DataFrame([
            ["the", "0.6343,0.8561,0.1249,0.4701"],
            ["training", "0.2753,0.2444,0.3699,0.6048"],
            ["of", "0.3160,0.3675,0.1649,0.4116"],
            ["increases", "1.0372,0.6092,0.1050,0.2630"],
            ["word", "0.9911,0.6338,0.4570,0.8451"],
            ["vectors", "0.8780,0.4500,0.5455,0.7495"],
            ["speed", "0.9504,0.3168,0.7484,0.6965"],
            ["significantly", "-0.0465,0.6597,0.0906,0.7137"],
            ["quality", "0.9745,0.7521,0.8874,0.5192"],
            ["is", "0.8221,0.0487,-0.0065,0.4088"],
            ["improved", "0.1910,0.0723,0.8216,0.4367"],
            ["data", "0.8985,0.0117,0.8083,0.9636"],
            ["amount", "0.9786,0.1470,0.7385,0.8856"]
        ])
        
        modelOp = BatchOperator.fromDataframe(df2, schemaStr="id string, vec string")
        
        AggLookupBatchOp() \
            .setClause("CONCAT(sentence,2) as concat, AVG(sentence) as avg, SUM(sentence) as sum,MAX(sentence) as max,MIN(sentence) as min") \
            .setDelimiter(" ") \
            .setReservedCols([]) \
            .linkFrom(modelOp, inOp) \
            .print()
        pass