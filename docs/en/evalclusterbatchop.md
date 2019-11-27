## Description
Calculate the cluster evaluation metrics for clustering.
 
 PredictionCol is required for evaluation. LabelCol is optional, if given, NMI/Purity/RI/ARI will be calcuated.
 VectorCol is also optional, if given, SilhouetteCoefficient/SSB/SSW/Compactness/SEPERATION/DAVIES_BOULDIN
 /CALINSKI_HARABAZ will be calculated. If only predictionCol is given, only K/ClusterArray/CountArray will be
 calculated.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| labelCol | Name of the label column in the input table | String |  | null |
| vectorCol | Name of a vector column | String |  | null |
| predictionCol | Column name of prediction. | String | ✓ |  |
| distanceType | Distance type for clustering, support EUCLIDEAN and COSINE. | String |  | "EUCLIDEAN" |


## Script Example
#### Code

```
import numpy as np
import pandas as pd
data = np.array([
    [0, "0 0 0"],
    [0, "0.1,0.1,0.1"],
    [0, "0.2,0.2,0.2"],
    [1, "9 9 9"],
    [1, "9.1 9.1 9.1"],
    [1, "9.2 9.2 9.2"]
])
df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})
inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')

metrics = EvalClusterBatchOp().setVectorCol("vec").setPredictionCol("id").linkFrom(inOp).collectMetrics()

print("Total Samples Number:", metrics.getCount())
print("Cluster Number:", metrics.getK())
print("Cluster Array:", metrics.getClusterArray())
print("Cluster Count Array:", metrics.getCountArray())
print("CP:", metrics.getCompactness())
print("DB:", metrics.getDaviesBouldin())
print("SP:", metrics.getSeperation())
print("SSB:", metrics.getSsb())
print("SSW:", metrics.getSsw())
print("CH:", metrics.getCalinskiHarabaz())
```

#### Results
```
Total Samples Number: 6
Cluster Number: 2
Cluster Array: ['0', '1']
Cluster Count Array: [3.0, 3.0]
CP: 0.11547005383792497
DB: 0.014814814814814791
SP: 15.588457268119896
SSB: 364.5
SSW: 0.1199999999999996
CH: 12150.000000000042
```
