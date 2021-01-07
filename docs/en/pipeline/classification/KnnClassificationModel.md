## Description
Knn classification model fitted by KnnClassifier.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| k | k | Integer |  | 10 |
| predictionCol | Column name of prediction. | String | ✓ |  |
| vectorCol | Name of a vector column | String |  | null |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |


