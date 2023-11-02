import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVertexNeighborSearchBatchOp(unittest.TestCase):
    def test_vertexneighborsearchbatchop(self):

        df = pd.DataFrame([
            ["Alice", "Lisa", 1., "hello"],
        		["Lisa", "Alice", 1., "hello"],
        		["Lisa", "Karry", 1., "hello"],
        		["Karry", "Lisa", 1., "213"],
        		["Karry", "Bella", 1., "hello"],
        		["Bella", "Karry", 1., "h123ello"],
        		["Bella", "Lucy", 1., "hello"],
        		["Lucy", "Bella", 1., "hello"],
        		["Lucy", "Bob", 1., "123"],
        		["Bob", "Lucy", 1., "hello"],
        		["John", "Bob", 1., "hello"],
        		["Bob", "John", 1., "hello"],
        		["John", "Stella", 1., "123"],
        		["Stella", "John", 1., "hello"],
        		["Kate", "Stella", 1., "hello"],
        		["Stella", "Kate", 1., "hello"],
        		["Kate", "Jack", 1., "13"],
        		["Jack", "Kate", 1., "hello"],
        		["Jess", "Jack", 1., "13"],
        		["Jack", "Jess", 1., "hello"],
        		["Jess", "Jacob", 1., "hello"],
        		["Jacob", "Jess", 1., "123"]]
        )
        data = BatchOperator.fromDataframe(df, schemaStr="start string, end string, value double, attr string")
        op = VertexNeighborSearchBatchOp() \
            .setDepth(1) \
            .setSources(["John", "Lisa"]) \
            .setEdgeSourceCol("start") \
            .setEdgeTargetCol("end") \
            .setVertexIdCol("name") \
            .setAsUndirectedGraph(False)
        op.linkFrom(data).print()
        pass