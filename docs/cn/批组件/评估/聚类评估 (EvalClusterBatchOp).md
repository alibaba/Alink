# 聚类评估 (EvalClusterBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp

Python 类名：EvalClusterBatchOp


## 功能介绍
对聚类算法的预测结果进行效果评估。

### 算法原理

在下面的指标中，用 $C_i$ 表示第 $i$ 个簇， $u_i$ 表示$C_i$ 的中心，$k$ 表示簇的总数。

#### Compactness(CP)

$\overline{CP_i}=\dfrac{1}{|C_i|}\sum_{x \in C_i}\|x_i-u_i\|$

$\overline{CP}=\dfrac{1}{k}\sum_{i=1}^{k}\overline{CP_k}$

CP越低意味着类内聚类距离越近。

#### Separation (SP)

$SP=\dfrac{2}{k^2-k}\sum_{i=1}^{k}\sum_{j=i+1}^{k}\|u_i-u_j\|$

SP越高意味类间聚类距离越远。

#### Davies-Bouldin Index (DB)

$DB=\dfrac{1}{k}\sum_{i=1}^{k}\max_{i \not=j}(\dfrac{\overline{CP_i}+\overline{CP_j}}{\|u_i-u_j\|})$

DB越小意味着类内距离越小，同时类间距离越大。

#### Calinski-Harabasz Index (Variance Ratio Criterion, VRC)

用 $u$ 表示数据集整体的中心点，

$SSB=\sum_{i=1}^{k}n_i\|u_i-u\|^2,$

$SSW=\sum_{i=1}^{k}\sum_{x \in C_i}\|x_i-u_i\|,$

$\mathrm{VRC}=\dfrac{SSB}{SSW}*\dfrac{N-k}{k-1}$

VRC越大意味着聚类质量越好。


另有一部分聚类评价指标称作外部指标（external criterion）。
这些指标在评估时除了有每个样本点所属的簇之外，还假设每个样本点有类别标签。

在下面的指标中，$N$表示簇的总数，用 $\omega_k$ 表示簇$k$所包含的样本点集合，$c_j$ 表示类别标签为 $j$ 的样本点集合。

#### Purity

$Purity(\Omega, C)=\dfrac{1}{N}\sum_{k}\max_{j}|\omega_k \cap c_j|$

取值在 $[0,1]$ 区间内,越接近1表示同一个簇内相同类别的数据点越多，聚类结果越好。

#### Normalized Mutual Information (NMI), NMI在[0,1]区间内, 越接近1表示聚类结果越好

$H(\Omega)=-\sum_{k}\dfrac{\omega_k}{N}log\dfrac{\omega_k}{N},$

$H(C)=-\sum_{j}\dfrac{c_j}{N}log\dfrac{c_j}{N},$

$I(\Omega, C)=\sum_k\sum_j\dfrac{|\omega_k \cap c_j|}{N}\log \dfrac{N|\omega_k \cap c_j|}{|\omega_k||c_j|},$

$\mathrm{NMI}=\dfrac{2 * I(\Omega, C)}{H(\Omega) + H(C)}$

取值在 $[0,1]$ 区间内, 越接近1表示聚类结果越好。

#### Rand Index (RI)

对于任意一对样本点：
  - 如果标签相同并且属于相同的簇，则认为是 TP (True Positive)； 
  - 如果标签不同并且属于不同的簇，则认为是 TN (True Negative)； 
  - 如果标签相同并且属于不同的簇，则认为是 FN (False Negative)；
  - 如果标签不同并且属于相同的簇，则认为是 FP (False Positive)。

用 $TP, TN, FN, FP$ 分别表示属于各自类别的样本点对的个数，$N(k,j)$ 表示簇 $k$ 内类别为 $j$ 的样本点个数，那么有：

$TP+FP=\sum_{j}\binom{c_j}{2},$

$TP+FN=\sum_{k}\binom{\omega_k}{2},$

$TP=\sum_{k}\sum_{j}\binom{N(k,j)}{2},$

$TP+TN+FP+FN= \binom{N}{2},$

$RI=\dfrac{TP+TN}{TP+TN+FP+FN}$

取值在 $[0,1]$ 区间内，越接近1表示聚类结果越好。

#### Adjusted Rand Index(ARI), ARI在[-1,1]区间内,越接近1表示聚类结果越好

$\mathrm{Index}=TP,$

$\mathrm{ExpectedIndex}=\dfrac{(TP+FP)(TP+FN)}{TP+TN+FP+FN},$

$\mathrm{MaxIndex}=\dfrac{TP+FP+TP+FN}{2},$

$ARI=\dfrac{\mathrm{Index} - \mathrm{ExpectedIndex}}{\mathrm{MaxIndex} - \mathrm{ExpectedIndex}}$
E
取值在 $[-1,1]$ 区间内，越接近1表示聚类结果越好。

### 使用方式

该组件通常接聚类算法的输出端。

使用时，需要通过 `predictionCol` 指定预测结果类。 通常还需要通过 `vectorCol` 指定样本点的坐标，这样才能计算评估指标。否则，只能输出样本点所属簇等基本信息。
另外，可以根据需要指定标签列 `labelCol`，这样可以计算外部指标。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| distanceType | 距离度量方式 | 距离类型 | String |  | "EUCLIDEAN", "COSINE", "CITYBLOCK" | "EUCLIDEAN" |
| labelCol | 标签列名 | 输入表中的标签列名 | String |  |  | null |
| vectorCol | 向量列名 | 输入表中的向量列名 | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, "0 0 0"],
    [0, "0.1,0.1,0.1"],
    [0, "0.2,0.2,0.2"],
    [1, "9 9 9"],
    [1, "9.1 9.1 9.1"],
    [1, "9.2 9.2 9.2"]
])

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
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.common.evaluation.ClusterMetrics;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EvalClusterBatchOpTest {
	@Test
	public void testEvalClusterBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "0 0 0"),
			Row.of(0, "0.1,0.1,0.1"),
			Row.of(0, "0.2,0.2,0.2"),
			Row.of(1, "9 9 9"),
			Row.of(1, "9.1 9.1 9.1"),
			Row.of(1, "9.2 9.2 9.2")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, vec string");
		ClusterMetrics metrics = new EvalClusterBatchOp().setVectorCol("vec").setPredictionCol("id").linkFrom(inOp)
			.collectMetrics();
		System.out.println("Total Samples Number:" + metrics.getCount());
		System.out.println("Cluster Number:" + metrics.getK());
		System.out.println("Cluster Array:" + Arrays.toString(metrics.getClusterArray()));
		System.out.println("Cluster Count Array:" + Arrays.toString(metrics.getCountArray()));
		System.out.println("CP:" + metrics.getCp());
		System.out.println("DB:" + metrics.getDb());
		System.out.println("SP:" + metrics.getSp());
		System.out.println("SSB:" + metrics.getSsb());
		System.out.println("SSW:" + metrics.getSsw());
		System.out.println("CH:" + metrics.getVrc());
	}
}
```

### 运行结果
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
