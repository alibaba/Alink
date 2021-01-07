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
| vectorCol | Name of a vector column | String | ✓ |  |
| k | Number of clusters. | Integer |  | 2 |
| maxIter | Maximum iterations, The default value is 100 | Integer |  | 100 |
| epsilon | When the distance between two rounds of centers is lower than epsilon, we consider the algorithm converges! | Double |  | 1.0E-4 |
| randomSeed | Random seed, it should be positive integer | Integer |  | 0 |

## Script Example
### Code
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

df_data = pd.DataFrame({
    "features": data[:, 0],
})

data = dataframeToOperator(df_data, schemaStr='features string', op_type='batch')

gmm = GmmTrainBatchOp() \
    .setVectorCol("features") \
    .setEpsilon(0.)

model = gmm.linkFrom(data)
model.print()
```

### Results

```
   model_id                                         model_info
0         0  {"vectorCol":"\"features\"","numFeatures":"2",...
1   1048576  {"clusterId":0,"weight":0.7354489748549162,"me...
2   2097152  {"clusterId":1,"weight":0.26455102514508383,"m...
```
