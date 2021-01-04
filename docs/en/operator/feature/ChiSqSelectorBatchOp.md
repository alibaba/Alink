## Description
chi-square selector for table.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| selectorType | The selector supports different selection methods: `NumTopFeatures`, `percentile`, `fpr`,
  `fdr`, `fwe`.
   - `NumTopFeatures` chooses a fixed number of top features according to a chi-squared test.
   - `percentile` is similar but chooses a fraction of all features instead of a fixed number.
   - `fpr` chooses all features whose p-values are below a threshold, thus controlling the false
     positive rate of selection.
   - `fdr` uses the [Benjamini-Hochberg procedure]
     (https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure)
     to choose all features whose false discovery rate is below a threshold.
   - `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by
     1/numFeatures, thus controlling the family-wise error rate of selection.
  By default, the selection method is `NumTopFeatures`, with the default number of top features | String |  | "NumTopFeatures" |
| numTopFeatures | Number of features that selector will select, ordered by ascending p-value. If the number of features is < NumTopFeatures, then this will select all features.  By default, 50 | Integer |  | 50 |
| percentile | Percentile of features that selector will select, ordered by ascending p-value. It must be in range (0,1)  By default, 0.1 | Double |  | 0.1 |
| fpr | The highest p-value for features to be kept. It must be in range (0,1)  By default, 0.05 | Double |  | 0.05 |
| fdr | The upper bound of the expected false discovery rate.It must be in range (0,1)  By default, 0.05 | Double |  | 0.05 |
| fwe | The upper bound of the expected family-wise error rate. rate.It must be in range (0,1)  By default, 0.05 | Double |  | 0.05 |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |
| labelCol | Name of the label column in the input table | String | ✓ |  |

## Script Example

#### Code
```python

data = np.array([
    ["a", 1, 1,2.0, True],
    ["c", 1, 2, -3.0, True],
    ["a", 2, 2,2.0, False],
    ["c", 0, 0, 0.0, False]
])

df = pd.DataFrame({"f_string": data[:, 0], "f_long": data[:, 1], "f_int": data[:, 2], "f_double": data[:, 3], "f_boolean": data[:, 4]})
source = dataframeToOperator(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean', op_type="batch")

selector = ChiSqSelectorBatchOp()\
            .setSelectedCols(["f_string", "f_long", "f_int", "f_double"])\
            .setLabelCol("f_boolean")\
            .setNumTopFeatures(2)

selector.linkFrom(source)

modelInfo: ChisqSelectorModelInfo = selector.collectModelInfo()
        
print(modelInfo.getColNames())


```

#### Results

```
['f_string', 'f_long']
```




