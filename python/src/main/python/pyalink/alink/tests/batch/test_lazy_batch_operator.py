import io
import sys
import unittest

from pyalink.alink import *


class TestLazyBatchOperator(unittest.TestCase):

    def setUp(self) -> None:
        self.saved_stdout = sys.stdout
        self.str_out = io.StringIO()
        sys.stdout = self.str_out

    def tearDown(self) -> None:
        sys.stdout = self.saved_stdout
        print(self.str_out.getvalue())

    def test_lazy_print_in_execute(self):
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
        title = "===== LAZY_PRINT ====="
        source.lazyPrint(5, title)
        BatchOperator.execute()

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_lazy_print_in_collect(self):
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
        title = "===== LAZY_PRINT ====="
        source.lazyPrint(5, title)
        print(source.collectToDataframe())

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_lazy_print_in_print(self):
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
        title = "===== LAZY_PRINT ====="
        source.lazyPrint(5, title)
        source.print()

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_lazy_collect_in_print(self):
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
        title = "===== CALLBACK in LAZY_COLLECT ====="
        source.lazyCollectToDataframe(
            lambda df: print(title, df),
            lambda df: self.assertEqual(type(df), pd.DataFrame)
        )
        source.print()

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    # TODO
    def test_exception_in_lazy_collect(self):
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
        title = "===== CALLBACK in LAZY_COLLECT ====="
        source.lazyCollectToDataframe(
            lambda df: print(df),
            # lambda df: self.fail()
        )
        source.print()
        #
        # content = self.str_out.getvalue()
        # self.assertTrue(title in content)

    def test_lazy_collect_statistics(self):
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
        title1 = "===== CALLBACK in LAZY_COLLECT ====="
        source.lazyCollectToDataframe(lambda df: print(title1, df))
        title2 = "===== CALLBACK in LAZY_COLLECT_STATISTICS ====="
        source.lazyCollectStatistics(
            lambda stat: print(title2, stat, sep="\n"),
            lambda stat: self.assertEqual(type(stat), TableSummary),
        )
        BatchOperator.execute()

        content = self.str_out.getvalue()
        self.assertTrue(title1 in content)
        self.assertTrue(title2 in content)

    def test_lazy_viz_statistics(self):
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
        source.lazyVizStatistics()
        source.lazyVizStatistics("test")
        BatchOperator.execute()

    def test_lazy_viz_dive(self):
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
        source.lazyVizDive()
        BatchOperator.execute()

    def test_lazy_collect_statistics_before_link(self):
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
        source.link(
            SummarizerBatchOp().lazyPrintStatistics()
        )
        BatchOperator.execute()

    def test_multiple_execute(self):
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
        title = "===== CALLBACK in LAZY_PRINT ====="
        source.lazyPrint(-1, title)
        BatchOperator.execute()

        # should print source
        content = self.str_out.getvalue()
        self.assertTrue(title in content)

        self.str_out.truncate(0)
        self.str_out.seek(0)

        source = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        source.print()

        # should NOT print source
        content = self.str_out.getvalue()
        self.assertFalse(title in content)

    def test_multiple_link_execute(self):
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

        title = "===== CALLBACK in LAZY_PRINT ====="
        source.lazyPrint(-1, title)
        first_n = FirstNBatchOp().setSize(5).linkFrom(source)
        first_n.print()

        # should print source
        content = self.str_out.getvalue()
        self.assertTrue(title in content)

        self.str_out.truncate(0)
        self.str_out.seek(0)

        first_n = FirstNBatchOp().setSize(5).linkFrom(source)
        first_n.print()

        # should NOT print source
        content = self.str_out.getvalue()
        self.assertFalse(title in content)
