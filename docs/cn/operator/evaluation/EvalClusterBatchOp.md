## 功能介绍
聚类评估是对聚类算法的预测结果进行效果评估，支持下列评估指标。

#### Compactness(CP), CP越低意味着类内聚类距离越近
<div align=center><img src="http://latex.codecogs.com/gif.latex?\overline{CP_i}=\dfrac{1}{|C_i|}\sum_{x \in C_i}\|x_i-u_i\|" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?\overline{CP}=\dfrac{1}{k}\sum_{i=1}^{k}\overline{CP_k}" ></div>

#### Seperation(SP), SP越高意味类间聚类距离越远
<div align=center><img src="http://latex.codecogs.com/gif.latex?SP=\dfrac{2}{k^2-k}\sum_{i=1}^{k}\sum_{j=i+1}^{k}\|u_i-u_j\|" ></div>

#### Davies-Bouldin Index(DB), DB越小意味着类内距离越小 同时类间距离越大
<div align=center><img src="http://latex.codecogs.com/gif.latex?DB=\dfrac{1}{k}\sum_{i=1}^{k}max(\dfrac{\overline{CP_i}+\overline{CP_j}}{\|u_i-u_j\|}), i \not= j" ></div>


#### Calinski-Harabasz Index(VRC), VRC越大意味着聚类质量越好
<div align=center><img src="http://latex.codecogs.com/gif.latex?SSB=\sum_{i=1}^{k}n_i\|u_i-u\|^2" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?SSW=\sum_{i=1}^{k}\sum_{x \in C_i}\|x_i-u_i\|" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?VRC=\dfrac{SSB}{SSW}*\dfrac{N-k}{k-1}" ></div>

#### Purity, Putity在[0,1]区间内,越接近1表示聚类结果越好
<div align=center><img src="http://latex.codecogs.com/gif.latex?Purity(\Omega, C)=\dfrac{1}{N}\sum_{k}\underset jmax|\omega_k \cap c_j|" ></div>

#### Normalized Mutual Information(NMI), NMI在[0,1]区间内, 越接近1表示聚类结果越好
<div align=center><img src="http://latex.codecogs.com/gif.latex?H(\Omega)=-\sum_{k}\dfrac{\omega_k}{N}log\dfrac{\omega_k}{N}" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?H(C)=-\sum_{j}\dfrac{c_j}{N}log\dfrac{c_j}{N}" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?I(\Omega, C)=\sum_k\sum_j\dfrac{|\omega_k \cap c_j|}{N}log \dfrac{N|\omega_k \cap c_j|}{|\omega_k||c_j|}" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?NMI=\dfrac{2 * I(\Omega, C)}{H(\Omega) + H(C)}" ></div>

#### Rand Index(RI), RI在[0,1]区间内,越接近1表示聚类结果越好
<div align=center><img src="http://latex.codecogs.com/gif.latex?TP+FP=\sum_{j}(_{2}^{c_j})" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?TP+FN=\sum_{k}(_{2}^{\omega_k})" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?TP=\sum_{k}\sum_{j}(_{2}^{N(k,j)})" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?TP+TN+FP+FN=(_{2}^{N})" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?RI=\dfrac{TP+TN}{TP+TN+FP+FN}" ></div>

#### Adjusted Rand Index(ARI), ARI在[-1,1]区间内,越接近1表示聚类结果越好
<div align=center><img src="http://latex.codecogs.com/gif.latex?Index=TP" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?ExpectedIndex=\dfrac{(TP+FP)(TP+FN)}{TP+TN+FP+FN}" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?MaxIndex=\dfrac{TP+FP+TP+FN}{2}" ></div>
<div align=center><img src="http://latex.codecogs.com/gif.latex?ARI=\dfrac{Index - ExpectedIndex}{MaxIndex - ExpectedIndex}" ></div>

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String |  | null |
| vectorCol | 向量列名 | 输入表中的向量列名 | String |  | null |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| distanceType | 距离度量方式 | 距离类型 | String |  | "EUCLIDEAN" |



## 脚本示例
#### 脚本代码

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
print("CP:", metrics.getCp())
print("DB:", metrics.getDb())
print("SP:", metrics.getSp())
print("SSB:", metrics.getSsb())
print("SSW:", metrics.getSsw())
print("CH:", metrics.getVrc())
```

#### 脚本运行结果
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
