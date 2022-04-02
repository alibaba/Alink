import unittest


class TestGmm(unittest.TestCase):

    def test_gmm(self):
        pass
    #     data = np.array([
    #         ["football"],
    #         ["football"],
    #         ["football"],
    #         ["basketball"],
    #         ["basketball"],
    #         ["tennis"],
    #     ])
    #
    #     df_data = pd.DataFrame({
    #         "f0": data[:, 0],
    #     })
    #
    #     data = dataframeToOperator(df_data, schemaStr='f0 string', op_type='batch')
    #
    #     stringIndexer = StringIndexer() \
    #         .setModelName("string_indexer_model") \
    #         .setSelectedCol("f0") \
    #         .setOutputCol("f0_indexed") \
    #         .setStringOrderType("frequency_asc")
    #
    #     indexed = stringIndexer.fit(data).transform(data)
    #
    #     indexToString = IndexToString(model=None) \
    #         .setModelName("string_indexer_model") \
    #         .setSelectedCol("f0_indexed") \
    #         .setOutputCol("f0_indxed_unindexed")
    #     indexToString.transform(indexed).print()
