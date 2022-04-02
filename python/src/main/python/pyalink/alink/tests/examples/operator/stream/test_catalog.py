import os
import tempfile
import unittest

from pyalink.alink import *


class TestCatalog(unittest.TestCase):

    def test_catalog_stream_op(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            [1.1, "2", "A"],
            [1.1, "2", "B"],
            [1.1, "1", "B"],
            [2.2, "1", "A"]
        ])
        df = pd.DataFrame({"col0": data[:, 0], "col1": data[:, 1], "col2": data[:, 2]})
        source = StreamOperator.fromDataframe(df, schemaStr='col0 double, col1 int, col2 string')

        derby = DerbyCatalog("derby_catalog", None, "10.6.1.0", os.path.join(tempfile.TemporaryDirectory().name, "derby_db"))
        object_path = ObjectPath(database_name="test_catalog_source_sink", object_name="test_catalog_source_sink")
        derby.open()
        derby.drop_table(object_path, True)
        if derby.database_exists("test_catalog_source_sink"):
            derby.drop_database("test_catalog_source_sink", True)
        derby.close()

        catalog_object = CatalogObject(derby, object_path)

        catalog_sink = CatalogSinkStreamOp() \
            .setCatalogObject(catalog_object) \
            .linkFrom(source)
        StreamOperator.execute()

        catalog_source = CatalogSourceStreamOp() \
            .setCatalogObject(catalog_object)
        catalog_source.print()
        StreamOperator.execute()
