## Description
Gaussian Mixture is a kind of clustering algorithm.
 
 Gaussian Mixture clustering performs expectation maximization for multivariate Gaussian
 Mixture Models (GMMs).  A GMM represents a composite distribution of
 independent Gaussian distributions with associated "mixing" weights
 specifying each's contribution to the composite.
 
 Given a set of sample points, this class will maximize the log-likelihood
 for a mixture of k Gaussians, iterating until the log-likelihood changes by
 less than convergenceTol, or until it has reached the max number of iterations.
 While this process is generally guaranteed to converge, it is not guaranteed
 to find a global optimum.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| lazyPrintModelInfoEnabled | Enable lazyPrint of ModelInfo | Boolean |  | false |
| lazyPrintModelInfoTitle | Title of ModelInfo in lazyPrint | String |  | null |
| vectorCol | Name of a vector column | String | ✓ |  |
| k | Number of clusters. | Integer |  | 2 |
| maxIter | Maximum iterations, The default value is 100 | Integer |  | 100 |
| epsilon | When the distance between two rounds of centers is lower than epsilon, we consider the algorithm converges! | Double |  | 1.0E-4 |
| randomSeed | Random seed, it should be positive integer | Integer |  | 0 |
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
#### Code
```python
from pyalink.alink import *
import pandas as pd
import numpy as np

data = np.array([
    ["-0.6264538 0.1836433"],
    ["-0.8356286 1.5952808"],
    ["0.3295078 -0.8204684"],
    ["0.4874291 0.7383247"],
    ["0.5757814 -0.3053884"],
    ["1.5117812 0.3898432"],
    ["-0.6212406 -2.2146999"],
    ["11.1249309 9.9550664"],
    ["9.9838097 10.9438362"],
    ["10.8212212 10.5939013"],
    ["10.9189774 10.7821363"],
    ["10.0745650 8.0106483"],
    ["10.6198257 9.9438713"],
    ["9.8442045 8.5292476"],
    ["9.5218499 10.4179416"],
])

df_data = pd.DataFrame({"features": data[:, 0]})

data = dataframeToOperator(df_data, schemaStr='features string', op_type='batch')

gmm = GaussianMixture() \
    .setPredictionCol("cluster_id") \
    .setVectorCol("features") \
    .setPredictionDetailCol("cluster_detail") \
    .setEpsilon(0.)

gmm.fit(data).transform(data).print()
```

#### Results

```
                 features  cluster_id              cluster_detail
0    -0.6264538 0.1836433           0   1.0 4.275273913994525E-92
1    -0.8356286 1.5952808           0  1.0 1.0260377730321551E-92
2    0.3295078 -0.8204684           0  1.0 1.0970173367581376E-80
3     0.4874291 0.7383247           0   1.0 3.302173132326017E-75
4    0.5757814 -0.3053884           0   1.0 3.163811360527242E-76
5     1.5117812 0.3898432           0  1.0 2.1018052308784284E-62
6   -0.6212406 -2.2146999           0   1.0 6.772270268623849E-97
7    11.1249309 9.9550664           1  3.1567838012476857E-56 1.0
8    9.9838097 10.9438362           1  1.9024447346702428E-51 1.0
9   10.8212212 10.5939013           1   2.800973098729624E-56 1.0
10  10.9189774 10.7821363           1   1.720913274489149E-57 1.0
11   10.0745650 8.0106483           1  2.8642696635133534E-43 1.0
12   10.6198257 9.9438713           1   5.773273991940417E-53 1.0
13    9.8442045 8.5292476           1   2.527312305092689E-43 1.0
14   9.5218499 10.4179416           1  1.7314580596765851E-46 1.0
```
