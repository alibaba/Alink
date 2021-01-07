## Description
Generalized Linear Model. https://en.wikipedia.org/wiki/Generalized_linear_model.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| family | the name of family which is a description of the error distribution. Supported options: Gaussian, Binomial, Poisson, Gamma and Tweedie | String |  | "Gaussian" |
| variancePower | The power in the variance function of the Tweedie distribution. It describe the relationship between the variance and mean of the distribution | Double |  | 0.0 |
| link | The name of link functionSupported options: CLogLog, Identity, Inverse, log, logit, power, probit and sqrt | String |  | null |
| linkPower | Param for the index in the power link function.  | Double |  | 1.0 |
| offsetCol | The col name of offset | String |  | null |
| fitIntercept | Sets if we should fit the intercept | Boolean |  | true |
| regParam | Sets the regularization parameter for L2 regularization | Double |  | 0.0 |
| epsilon | epsilon | Double |  | 1.0E-5 |
| linkPredResultCol | link predict col name of output | String |  | null |
| lazyPrintModelInfoEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintModelInfoTitle | Title of ModelInfo in lazyPrint | String |  | null |
| weightCol | Name of the column indicating weight | String |  | null |
| maxIter | Maximum iterations, The default value is 10 | Integer |  | 10 |
| predictionCol | Column name of prediction. | String | ✓ |  |
| numThreads | Thread number of operator. | Integer |  | 1 |
| lazyPrintTransformDataEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformDataTitle | Title of ModelInfo in lazyPrint | String |  | null |
| lazyPrintTransformDataNum | Title of ModelInfo in lazyPrint | Integer |  | -1 |
| lazyPrintTransformStatEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintTransformStatTitle | Title of ModelInfo in lazyPrint | String |  | null |
| featureCols | Names of the feature columns used for training in the input table | String[] | ✓ |  |
| labelCol | Name of the label column in the input table | String | ✓ |  |

## Script Example
#### Code
```python

import numpy as np

# data
data = np.array([
    [1.6094,118.0000,69.0000,1.0000,2.0000],
    [2.3026,58.0000,35.0000,1.0000,2.0000],
    [2.7081,42.0000,26.0000,1.0000,2.0000],
    [2.9957,35.0000,21.0000,1.0000,2.0000],
    [3.4012,27.0000,18.0000,1.0000,2.0000],
    [3.6889,25.0000,16.0000,1.0000,2.0000],
    [4.0943,21.0000,13.0000,1.0000,2.0000],
    [4.3820,19.0000,12.0000,1.0000,2.0000],
    [4.6052,18.0000,12.0000,1.0000,2.0000]
])


df = pd.DataFrame({"u": data[:, 0], "lot1": data[:, 1], "lot2": data[:, 2], "offset": data[:, 3], "weights": data[:, 4]})
source = dataframeToOperator(df, schemaStr='u double, lot1 double, lot2 double, offset double, weights double', op_type='batch')

featureColNames = ["lot1", "lot2"]
labelColName = "u"

# train
glm = GeneralizedLinearRegression()\
                .setFamily("gamma")\
                .setLink("Log")\
                .setRegParam(0.3)\
                .setMaxIter(5)\
                .setFeatureCols(featureColNames)\
                .setLabelCol(labelColName)\
                .setPredictionCol("pred")

model = glm.fit(source)
#predict = model.transform(source)
eval2 = model.evaluate(source)

#predict.print()
eval2.print()

```

#### Results

 u |  lot1|  lot2 | offset | weights  |    pred
----|----|------|-----------|------|-----------    
0 | 1.6094 | 118.0 | 69.0  |   1.0    |  2.0 | 0.378525
1 | 2.3026 |  58.0 | 35.0  |   1.0  |    2.0 | 0.970639
2|  2.7081  | 42.0 | 26.0 |    1.0  |    2.0 | 1.126458
3 | 2.9957 |  35.0 | 21.0 |    1.0  |    2.0 | 1.227753
4 | 3.4012 |  27.0 | 18.0 |    1.0  |    2.0 | 1.258898
5 | 3.6889 |  25.0 | 16.0 |    1.0  |    2.0 | 1.305654
6 | 4.0943 |  21.0 | 13.0 |    1.0 |     2.0|  1.367991
7 | 4.3820 |  19.0 | 12.0 |    1.0 |     2.0 | 1.383571
8 | 4.6052 |  18.0 | 12.0 |    1.0  |    2.0 | 1.375774












