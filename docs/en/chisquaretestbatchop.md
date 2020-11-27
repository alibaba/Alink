## Description
Chi-square test is chi-square independence test.
 Chi-square independence test is to test whether two factors affect each other.
 Its zero hypothesis is that the two factors are independent of each other.
 More information on chi-square test: http://en.wikipedia.org/wiki/Chi-squared_test

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| labelCol | Name of the label column in the input table | String | ✓ |  |
| selectedCols | Names of the columns used for processing | String[] | ✓ |  |

## Script Example

### Code

```python
data = np.array([
            ['a1','b1','c1'],
            ['a1','b2','c1'],
            ['a1','b1','c2'],
            ['a2','b1','c1'],
            ['a2','b2','c2'],
            ['a2', 'b1','c1']])

df = pd.DataFrame({"x1": data[:, 0], "x2": data[:, 1], "x3": data[:, 2]})
source = dataframeToOperator(df, schemaStr='x1 string, x2 string, x3 string', op_type='batch')

chisqTest = ChiSquareTestBatchOp()\
            .setSelectedCols(["x1","x2"])\
            .setLabelCol("x3")

source.link(chisqTest).print()
```
### Results

col|chi2_result
-----|-----------
x1|{"comment":"pearson test","df":1.0,"p":1.0,"value":0.0}
x2|{"comment":"pearson test","df":1.0,"p":0.5402913746074196,"value":0.37500000000000006}




