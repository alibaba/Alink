import io
import sys
import unittest

from pyalink.alink import *


class TestLazyModelTrainInfo(unittest.TestCase):

    def setUp(self) -> None:
        self.saved_stdout = sys.stdout
        self.str_out = io.StringIO()
        sys.stdout = self.str_out

    def tearDown(self) -> None:
        sys.stdout = self.saved_stdout
        print(self.str_out.getvalue())

    def test_lazy_model_info(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            [0, "0 0 0"],
            [1, "0.1,0.1,0.1"],
            [2, "0.2,0.2,0.2"],
            [3, "9 9 9"],
            [4, "9.1 9.1 9.1"],
            [5, "9.2 9.2 9.2"]
        ])
        df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})
        source = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        train_op = KMeansTrainBatchOp().setK(2).setVectorCol("vec").linkFrom(source)
        title = "===== TITLE in LAZY_PRINT_MODEL_INFO ====="
        train_op.lazyPrintModelInfo(title)
        train_op.lazyCollectModelInfo(lambda d: (self.assertTrue(isinstance(d, KMeansModelInfo)), print(d.getClusterNumber())))
        BatchOperator.execute()

        model_info: KMeansModelInfo = train_op.collectModelInfo()
        self.assertTrue(isinstance(model_info, KMeansModelInfo))
        print(model_info.getClusterNumber())

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_lazy_train_info(self):
        import numpy as np
        import pandas as pd

        data = np.array([
            ["A B C"]
        ])

        df = pd.DataFrame({"tokens": data[:, 0]})
        source = dataframeToOperator(df, schemaStr='tokens string', op_type='batch')
        train_op = Word2VecTrainBatchOp().setSelectedCol("tokens").setMinCount(1).setVectorSize(4).linkFrom(source)
        title = "===== TITLE in LAZY_PRINT_TRAIN_INFO ====="
        train_op.lazyPrintTrainInfo(title)
        train_op.lazyCollectTrainInfo(lambda d: (self.assertTrue(isinstance(d, Word2VecTrainInfo)), print(d.getLoss())))
        BatchOperator.execute()

        model_info: Word2VecTrainInfo = train_op.collectTrainInfo()
        self.assertTrue(isinstance(model_info, Word2VecTrainInfo))
        print(model_info.getLoss())

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_model_train_info_before_link(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 2]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})

        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        model_info_title = "===== TITLE in LAZY_PRINT_MODEL_INFO ====="
        train_info_title = "===== TITLE in LAZY_PRINT_TRAIN_INFO ====="
        source.link(
            LinearSvmTrainBatchOp().setMaxIter(2).setFeatureCols(['f0', 'f1']).setLabelCol("label")
                .lazyCollectModelInfo(lambda d: print_with_title(d, model_info_title))
                .lazyCollectTrainInfo(lambda d: print_with_title(d, train_info_title))
        )
        BatchOperator.execute()

        content = self.str_out.getvalue()
        self.assertTrue(model_info_title in content)
        self.assertTrue(train_info_title in content)
