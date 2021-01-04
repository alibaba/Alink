## Description
EqualWidth discretizer keeps every interval the same width, output the interval
 as model, and can transform a new data using the model.
 The output is the index of the interval.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |
| outputCols | Names of the output columns | String[] |  | null |
| handleInvalid | Strategy to handle unseen token when doing prediction, one of "keep", "skip" or "error" | String |  | "KEEP" |
| encode | encode type: INDEX, VECTOR, ASSEMBLED_VECTOR. | String |  | "INDEX" |
| dropLast | drop last | Boolean |  | true |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |


