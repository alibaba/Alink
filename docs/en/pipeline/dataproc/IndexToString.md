## Description
Maps columns of indices to strings, based on the model fitted by {@link StringIndexer}.
 
 While {@link StringIndexerModel} maps string to index, IndexToString maps index to string.
 However, IndexToString does not have a corresponding {@link com.alibaba.alink.pipeline.EstimatorBase}.
 Instead, IndexToString uses model data in StringIndexerModel to perform predictions.
 
 IndexToString use the name of the {@link StringIndexerModel} to get the model data.
 The referenced {@link StringIndexerModel} should be created before calling <code>transform</code> method.
 
 A common use case is as follows:
 
 <code>
 StringIndexer stringIndexer = new StringIndexer()
 .setModelName("name_a") // The fitted StringIndexerModel will have name "name_a".
 .setSelectedCol(...);
 
 StringIndexerModel model = stringIndexer.fit(...); // This model will have name "name_a".
 
 IndexToString indexToString = new IndexToString()
 .setModelName("name_a") // Should match the name of one StringIndexerModel.
 .setSelectedCol(...)
 .setOutputCol(...);
 
 indexToString.transform(...); // Will relies on a StringIndexerModel with name "name_a" to do transformation.
 </code>
 
 The reason we use model name registration mechanism here is to make possible stacking both StringIndexer and
 IndexToString into a {@link Pipeline}. For examples,
 
 <code>
 StringIndexer stringIndexer = new StringIndexer()
 .setModelName("si_model_0").setSelectedCol("label");
 
 MultilayerPerceptronClassifier mlpc = new MultilayerPerceptronClassifier()
 .setVectorCol("features").setLabelCol("label").setPredictionCol("predicted_label");
 
 IndexToString indexToString = new IndexToString()
 .setModelName("si_model_0").setSelectedCol("predicted_label");
 
 Pipeline pipeline = new Pipeline().add(stringIndexer).add(mlpc).add(indexToString);
 
 pipeline.fit(...);
 </code>

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| modelName | Name of the model | String | ✓ |  |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCol | Name of the output column | String |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |

## Script Example
#### Code
```python
import numpy as np
import pandas as pd
data = np.array([
    ["football"],
    ["football"],
    ["football"],
    ["basketball"],
    ["basketball"],
    ["tennis"],
])

df_data = pd.DataFrame({
    "f0": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='f0 string', op_type="stream")
data_batch = dataframeToOperator(df_data, schemaStr='f0 string', op_type="batch")

stringIndexer = StringIndexer() \
    .setModelName("string_indexer_model") \
    .setSelectedCol("f0") \
    .setOutputCol("f0_indexed") \
    .setStringOrderType("frequency_asc")

indexed = stringIndexer.fit(data_batch).transform(data)

indexToString = IndexToString() \
    .setModelName("string_indexer_model") \
    .setSelectedCol("f0_indexed") \
    .setOutputCol("f0_indxed_unindexed")

indexToString.transform(indexed).print()

StreamOperator.execute()
```

#### Results

```
f0|f0_indexed|f0_indxed_unindexed
--|----------|-------------------
football|2|football
football|2|football
football|2|football
basketball|1|basketball
basketball|1|basketball
tennis|0|tennis
```
