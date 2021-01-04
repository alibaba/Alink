## Description
Key to Values operation with map model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| mapKeyCols | the names of the key column in map data table. | String[] | ✓ |  |
| mapValueCols | the names of the value column in map data table. | String[] | ✓ |  |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCols | Names of the output columns | String[] |  | null |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |


