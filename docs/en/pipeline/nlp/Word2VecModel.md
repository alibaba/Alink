## Description
It is word2vec model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCol | Name of the selected column used for processing | String | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCol | Name of the output column | String |  | null |
| wordDelimiter | Delimiter of words | String |  | " " |
| predMethod | Method to predict doc vector, support 3 method: avg, min and max, default value is avg. | String |  | "AVG" |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |


