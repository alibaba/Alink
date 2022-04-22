# 聚类评估 (EvalClusterBatchOp)
Java 类名：com.alibaba.alink.operator.batch.evaluation.EvalClusterBatchOp

Python 类名：EvalClusterBatchOp


## 功能介绍
聚类评估是对聚类算法的预测结果进行效果评估，支持下列评估指标。

#### Compactness(CP), CP越低意味着类内聚类距离越近
$$ \overline{CP_i}=\dfrac{1}{|C_i|}\sum_{x \in C_i}\|x_i-u_i\| $$

$$ \overline{CP}=\dfrac{1}{k}\sum_{i=1}^{k}\overline{CP_k} $$


#### Seperation(SP), SP越高意味类间聚类距离越远
$$ SP=\dfrac{2}{k^2-k}\sum_{i=1}^{k}\sum_{j=i+1}^{k}\|u_i-u_j\| $$


#### Davies-Bouldin Index(DB), DB越小意味着类内距离越小 同时类间距离越大
$$ DB=\dfrac{1}{k}\sum_{i=1}^{k}max(\dfrac{\overline{CP_i}+\overline{CP_j}}{\|u_i-u_j\|}), i \not= j $$



#### Calinski-Harabasz Index(VRC), VRC越大意味着聚类质量越好
$$ SSB=\sum_{i=1}^{k}n_i\|u_i-u\|^2 $$

$$ SSW=\sum_{i=1}^{k}\sum_{x \in C_i}\|x_i-u_i\| $$

$$ VRC=\dfrac{SSB}{SSW}*\dfrac{N-k}{k-1} $$


#### Purity, Putity在[0,1]区间内,越接近1表示聚类结果越好
$$ Purity(\Omega, C)=\dfrac{1}{N}\sum_{k}\underset jmax|\omega_k \cap c_j| $$


#### Normalized Mutual Information(NMI), NMI在[0,1]区间内, 越接近1表示聚类结果越好
$$ H(\Omega)=-\sum_{k}\dfrac{\omega_k}{N}log\dfrac{\omega_k}{N} $$

$$ H(C)=-\sum_{j}\dfrac{c_j}{N}log\dfrac{c_j}{N} $$

$$ I(\Omega, C)=\sum_k\sum_j\dfrac{|\omega_k \cap c_j|}{N}log \dfrac{N|\omega_k \cap c_j|}{|\omega_k||c_j|} $$

$$ NMI=\dfrac{2 * I(\Omega, C)}{H(\Omega) + H(C)} $$


#### Rand Index(RI), RI在[0,1]区间内,越接近1表示聚类结果越好
$$ TP+FP=\sum_{j}(_{2}^{c_j}) $$

$$ TP+FN=\sum_{k}(_{2}^{\omega_k}) $$

$$ TP=\sum_{k}\sum_{j}(_{2}^{N(k,j)}) $$

$$ TP+TN+FP+FN=(_{2}^{N}) $$

$$ RI=\dfrac{TP+TN}{TP+TN+FP+FN} $$


#### Adjusted Rand Index(ARI), ARI在[-1,1]区间内,越接近1表示聚类结果越好
$$ Index=TP $$

$$ ExpectedIndex=\dfrac{(TP+FP)(TP+FN)}{TP+TN+FP+FN} $$

$$ MaxIndex=\dfrac{TP+FP+TP+FN}{2} $$

$$ ARI=\dfrac{Index - ExpectedIndex}{MaxIndex - ExpectedIndex} $$


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
