import unittest

from pyalink.alink import *


class Evaluation(unittest.TestCase):

    def test_BinaryClassEvaluation(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            ["prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"],
            ["prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"],
            ["prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"],
            ["prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"],
            ["prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"]])

        df = pd.DataFrame({"label": data[:, 0], "detailInput": data[:, 1]})
        inOp = BatchOperator.fromDataframe(df, schemaStr='label string, detailInput string')

        EvalBinaryClassBatchOp().setLabelCol("label").setPredictionDetailCol("detailInput").linkFrom(inOp)\
            .lazyCollectMetrics(lambda d: print("~~~~", d))

        from pyalink.alink.common.types.metrics import BinaryClassMetrics
        metrics: BinaryClassMetrics = EvalBinaryClassBatchOp().setLabelCol("label").setPredictionDetailCol("detailInput")\
            .linkFrom(inOp).collectMetrics()
        self.assertEqual(type(metrics), BinaryClassMetrics)
        print("AUC:", metrics.getAuc())
        print("KS:", metrics.getKs())
        print("PRC:", metrics.getPrc())
        print("Accuracy:", metrics.getAccuracy())
        print("Macro Precision:", metrics.getMacroPrecision())
        print("Micro Recall:", metrics.getMicroRecall())
        print("Weighted Sensitivity:", metrics.getWeightedSensitivity())

        inOp = StreamOperator.fromDataframe(df, schemaStr='label string, detailInput string')
        EvalBinaryClassStreamOp().setLabelCol("label").setPredictionDetailCol("detailInput").setTimeInterval(1)\
            .linkFrom(inOp).print()
        StreamOperator.execute()

    def test_ClusterEvaluation(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            [0, "0 0 0"],
            [0, "0.1,0.1,0.1"],
            [0, "0.2,0.2,0.2"],
            [1, "9 9 9"],
            [1, "9.1 9.1 9.1"],
            [1, "9.2 9.2 9.2"]
        ])
        df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})
        inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')

        from pyalink.alink.common.types.metrics import ClusterMetrics
        metrics: ClusterMetrics = EvalClusterBatchOp().setVectorCol("vec").setPredictionCol("id")\
            .linkFrom(inOp).collectMetrics()
        self.assertEqual(type(metrics), ClusterMetrics)

        print("Total Samples Number:", metrics.getCount())
        print("Cluster Number:", metrics.getK())
        print("Cluster Array:", metrics.getClusterArray())
        print("Cluster Count Array:", metrics.getCountArray())
        print("CP:", metrics.getCp())
        print("DB:", metrics.getDb())
        print("SP:", metrics.getSp())
        print("SSB:", metrics.getSsb())
        print("SSW:", metrics.getSsw())
        print("CH:", metrics.getVrc())

    def test_MultiClassEvaluation(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            ["prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"],
            ["prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"],
            ["prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"],
            ["prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"],
            ["prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"]])

        df = pd.DataFrame({"label": data[:, 0], "detailInput": data[:, 1]})
        inOp = BatchOperator.fromDataframe(df, schemaStr='label string, detailInput string')

        from pyalink.alink.common.types.metrics import MultiClassMetrics
        metrics: MultiClassMetrics = EvalMultiClassBatchOp().setLabelCol("label").setPredictionDetailCol("detailInput")\
            .linkFrom(inOp).collectMetrics()
        self.assertEqual(type(metrics), MultiClassMetrics)
        print("Prefix0 accuracy:", metrics.getAccuracy("prefix0"))
        print("Prefix1 recall:", metrics.getRecall("prefix1"))
        print("Macro Precision:", metrics.getMacroPrecision())
        print("Micro Recall:", metrics.getMicroRecall())
        print("Weighted Sensitivity:", metrics.getWeightedSensitivity())

        inOp = StreamOperator.fromDataframe(df, schemaStr='label string, detailInput string')
        EvalMultiClassStreamOp().setLabelCol("label").setPredictionDetailCol("detailInput").setTimeInterval(1)\
            .linkFrom(inOp).print()
        StreamOperator.execute()

    def test_RegressionEvaluation(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            [0, 0],
            [8, 8],
            [1, 2],
            [9, 10],
            [3, 1],
            [10, 7]
        ])
        df = pd.DataFrame({"pred": data[:, 0], "label": data[:, 1]})
        inOp = BatchOperator.fromDataframe(df, schemaStr='pred int, label int')

        from pyalink.alink.common.types.metrics import RegressionMetrics
        metrics: RegressionMetrics = EvalRegressionBatchOp().setPredictionCol("pred").setLabelCol("label")\
            .linkFrom(inOp).collectMetrics()
        self.assertEqual(type(metrics), RegressionMetrics)

        print("Total Samples Number:", metrics.getCount())
        print("SSE:", metrics.getSse())
        print("SAE:", metrics.getSae())
        print("RMSE:", metrics.getRmse())
        print("R2:", metrics.getR2())

    def test_MultiLabelEvaluation(self):
        import pandas as pd

        data = [
            ("{\"object\":\"[0.0, 1.0]\"}", "{\"object\":\"[0.0, 2.0]\"}"),
            ("{\"object\":\"[0.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"),
            ("{\"object\":\"[]\"}", "{\"object\":\"[0.0]\"}"),
            ("{\"object\":\"[2.0]\"}", "{\"object\":\"[2.0]\"}"),
            ("{\"object\":\"[2.0, 0.0]\"}", "{\"object\":\"[2.0, 0.0]\"}"),
            ("{\"object\":\"[0.0, 1.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"),
            ("{\"object\":\"[1.0]\"}", "{\"object\":\"[1.0, 2.0]\"}")
        ]

        df = pd.DataFrame.from_records(data)
        source = BatchOperator.fromDataframe(df, "pred string, label string")

        evalMultiLabelBatchOp: EvalMultiLabelBatchOp = EvalMultiLabelBatchOp().setLabelCol("label").setPredictionCol("pred")\
            .linkFrom(source)
        metrics: MultiLabelMetrics = evalMultiLabelBatchOp.collectMetrics()

        self.assertEqual(type(metrics), MultiLabelMetrics)
        self.assertAlmostEquals(metrics.getRecall(), 0.64, delta=0.01)
        self.assertAlmostEquals(metrics.getPrecision(), 0.66, delta=0.01)
        self.assertAlmostEquals(metrics.getAccuracy(), 0.54, delta=0.01)
        self.assertAlmostEquals(metrics.getF1(), 0.63, delta=0.01)
        self.assertAlmostEquals(metrics.getMicroF1(), 0.69, delta=0.01)
        self.assertAlmostEquals(metrics.getMicroPrecision(), 0.72, delta=0.01)
        self.assertAlmostEquals(metrics.getMicroRecall(), 0.66, delta=0.01)

    def test_RankingEvaluation(self):
        import pandas as pd

        data = [
            ("{\"object\":\"[1, 6, 2, 7, 8, 3, 9, 10, 4, 5]\"}", "{\"object\":\"[1, 2, 3, 4, 5]\"}"),
            ("{\"object\":\"[4, 1, 5, 6, 2, 7, 3, 8, 9, 10]\"}", "{\"object\":\"[1, 2, 3]\"}"),
            ("{\"object\":\"[1, 2, 3, 4, 5]\"}", "{\"object\":\"[]\"}")
        ]
        df = pd.DataFrame.from_records(data)
        inOp = BatchOperator.fromDataframe(df, schemaStr='pred string, label string')
        metrics: RankingMetrics = EvalRankingBatchOp().setPredictionCol('pred').setLabelCol('label')\
            .linkFrom(inOp)\
            .collectMetrics()

        self.assertEqual(type(metrics), RankingMetrics)
        self.assertAlmostEquals(metrics.getNdcg(3), 0.33, delta=0.01)
        self.assertAlmostEquals(metrics.getNdcg(7), 0.42, delta=0.01)
        self.assertAlmostEquals(metrics.getNdcg(10), 0.48, delta=0.01)
        self.assertAlmostEquals(metrics.getNdcg(17), 0.48, delta=0.01)
        self.assertAlmostEquals(metrics.getPrecisionAtK(1), 0.33, delta=0.01)
        self.assertAlmostEquals(metrics.getPrecisionAtK(3), 0.33, delta=0.01)
        self.assertAlmostEquals(metrics.getPrecisionAtK(5), 0.26, delta=0.01)
        self.assertAlmostEquals(metrics.getPrecisionAtK(7), 0.28, delta=0.01)
        self.assertAlmostEquals(metrics.getPrecisionAtK(9), 0.26, delta=0.01)
        self.assertAlmostEquals(metrics.getPrecisionAtK(10), 0.26, delta=0.01)
        self.assertAlmostEquals(metrics.getPrecisionAtK(15), 0.17, delta=0.01)
        self.assertAlmostEquals(metrics.getMap(), 0.35, delta=0.01)
        self.assertAlmostEquals(metrics.getRecallAtK(1), 0.06, delta=0.01)

    def test_TimeSeriesEvaluation(self):
        import datetime
        import pandas as pd

        data = pd.DataFrame([
            [1, datetime.datetime.fromtimestamp(1), 10.0, 10.5],
            [1, datetime.datetime.fromtimestamp(2), 11.0, 10.5],
            [1, datetime.datetime.fromtimestamp(3), 12.0, 11.5],
            [1, datetime.datetime.fromtimestamp(4), 13.0, 12.5],
            [1, datetime.datetime.fromtimestamp(5), 14.0, 13.5],
            [1, datetime.datetime.fromtimestamp(6), 15.0, 14.5],
            [1, datetime.datetime.fromtimestamp(7), 16.0, 14.5],
            [1, datetime.datetime.fromtimestamp(8), 17.0, 14.5],
            [1, datetime.datetime.fromtimestamp(9), 18.0, 14.5],
            [1, datetime.datetime.fromtimestamp(10), 19.0, 16.5]
        ])
        source = BatchOperator.fromDataframe(data, schemaStr='id int, ts timestamp, val double, pred double')
        metrics: TimeSeriesMetrics = EvalTimeSeriesBatchOp().setLabelCol("val").setPredictionCol("pred") \
            .linkFrom(source) \
            .collectMetrics()
        print(metrics)
        self.assertEqual(type(metrics), TimeSeriesMetrics)
        self.assertAlmostEquals(metrics.getMse(), 2.85, delta=0.01)
        self.assertAlmostEquals(metrics.getRmse(), 1.6882, delta=0.01)
        self.assertAlmostEquals(metrics.getMae(), 1.3, delta=0.01)
        self.assertAlmostEquals(metrics.getMse(), 2.85, delta=0.01)
        self.assertAlmostEquals(metrics.getMape(), 8.1146, delta=0.01)
        self.assertAlmostEquals(metrics.getSmape(), 8.6064, delta=0.01)
        self.assertAlmostEquals(metrics.getND(), 0.0897, delta=0.01)
