## Description
Text Naive Bayes Classifier.
 
 We support the multinomial Naive Bayes and multinomial Naive Bayes model, a probabilistic learning method.
 Here, feature values of train table must be nonnegative.
 
 Details info of the algorithm:
 https://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| modelType | model type : Multinomial or Bernoulli. | String |  | "Multinomial" |
| lazyPrintModelInfoEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintModelInfoTitle | Title of ModelInfo in lazyPrint | String |  | null |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| weightCol | Name of the column indicating weight | String |  | null |
| vectorCol | Name of a vector column | String | ✓ |  |
| smoothing | the smoothing factor | Double |  | 1.0 |
| vectorCol | Name of a vector column | String | ✓ |  |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| predictionDetailCol | Column name of prediction result, it will include detailed info. | String |  |  |
| reservedCols | Names of the columns to be retained in the output table | String[] |  | null |

## Script Example
### Script
``` python
data =  np.array([
    ["$31$0:1.0 1:1.0 2:1.0 30:1.0","1.0  1.0  1.0  1.0", '1'],
    ["$31$0:1.0 1:1.0 2:0.0 30:1.0","1.0  1.0  0.0  1.0", '1'],
    ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
    ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
    ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
    ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
    ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0']])

dataSchema = ["sv", "dv", "label"]

df = pd.DataFrame({"sv": data[:, 0], "dv": data[:, 1], "label": data[:, 2]})
# batch data
batchData = dataframeToOperator(df, schemaStr='sv string, dv string, label string', op_type='batch')
# pipeline
model = NaiveBayesTextClassifier().setVectorCol("sv").setLabelCol("label").setReservedCols(["sv", "label"]).setPredictionCol("pred")
model.fit(batchData).transform(batchData).print()
```
### Result

sv | label | pred
---|-------|----
"$31$0:1.0 1:1.0 2:1.0 30:1.0"|1|1
"$31$0:1.0 1:1.0 2:0.0 30:1.0"|1|1
"$31$0:1.0 1:0.0 2:1.0 30:1.0"|1|1
"$31$0:1.0 1:0.0 2:1.0 30:1.0"|1|1
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0
"$31$0:0.0 1:1.0 2:1.0 30:0.0"|0|0

