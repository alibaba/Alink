# Agnes (AgnesBatchOp)
Java 类名：com.alibaba.alink.operator.batch.clustering.AgnesBatchOp

Python 类名：AgnesBatchOp


## 功能介绍
[AGNES](https://en.wikibooks.org/wiki/Data_Mining_Algorithms_In_R/Clustering/Hierarchical_Clustering)，AGglomerative NESting，是一个比较有代表性的`凝聚`的层次聚类。最开始的时候将所有数据点本身作为簇，然后找出距离最近的两个簇将它们合为一个，不断重复以上步骤直到达到预设的簇的个数或者簇之间距离大于一个距离阈值。&lt;br /&gt;
![](https://cdn.nlark.com/lark/0/2018/png/18345/1544081958424-d3803635-4542-4ae9-9635-603ef66945f2.png#alt=%E5%B1%8F%E5%B9%95%E5%BF%AB%E7%85%A7%202018-12-06%20%E4%B8%8B%E5%8D%883.38.49.png)

##### 距离度量方式
| 参数名称 | 参数描述 | 说明 |
| --- | --- | --- |
| EUCLIDEAN | ![](https://zos.alipayobjects.com/rmsportal/yVXAleRfvuwhJWJ.png#alt=image) | 欧式距离 |
| COSINE | ![](https://zos.alipayobjects.com/rmsportal/nmYGZXRGLqpQhbX.png#alt=image) | 夹角余弦距离 |
| CITYBLOCK | ![](https://zos.alipayobjects.com/rmsportal/jkApzArpNWDJFbV.png#alt=image) | 城市街区距离，也称曼哈顿距离 |

##### 时间复杂度

假定在开始的时候有N个簇，在结束的时候有K个簇，因此在主循环中有(N-K)次迭代，在第i次迭代中，我们必须在n−i+1个簇中找到最靠近的两个进行合并。另外算法必须计算所有对象两两之间的距离，如果使用簇间距离度量方式为COSIN，那这个算法的复杂度为 O(N^2 * (N-K) * D) ，该算法对于较大的情况是不适用的。此外，目前AGNES中只支持串行执行。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| idCol | id列名 | id列名 | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| vectorCol | 向量列名 | 向量列对应的列名 | String | ✓ | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] |  |
| distanceThreshold | 距离阈值 | 距离阈值 | Double |  |  | 1.7976931348623157E308 |
| distanceType | 距离度量方式 | 距离类型 | String |  | "EUCLIDEAN", "COSINE", "CITYBLOCK" | "EUCLIDEAN" |
| k | 聚类中心点数量 | 聚类中心点数量 | Integer |  |  | 2 |
| linkage | 类的聚合方式 | 类的聚合方式 | String |  | "MIN", "MAX", "MEAN", "AVERAGE" | "MIN" |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
        ["id_1", "2.0,3.0"],
        ["id_2", "2.1,3.1"],
        ["id_3", "200.1,300.1"],
        ["id_4", "200.2,300.2"],
        ["id_5", "200.3,300.3"],
        ["id_6", "200.4,300.4"],
        ["id_7", "200.5,300.5"],
        ["id_8", "200.6,300.6"],
        ["id_9", "2.1,3.1"],
        ["id_10", "2.1,3.1"],
        ["id_11", "2.1,3.1"],
        ["id_12", "2.1,3.1"],
        ["id_16", "300.,3.2"]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id string, vec string')

agnes = AgnesBatchOp()\
    .setIdCol("id")\
    .setVectorCol("vec")\
    .setPredictionCol("pred")\
    .linkFrom(inOp)
    
agnes.print()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.clustering.AgnesBatchOp;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AgnesBatchOpTest {

	@Test
	public void testAgnesBatchOp() throws Exception {
		List<Row> trainData = Arrays.asList(
    		Row.of("id_1", "2.0,3.0"),
	    	Row.of("id_2", "2.1,3.1"),
   		    Row.of("id_3", "200.1,300.1"),
	    	Row.of("id_4", "200.2,300.2"),
	    	Row.of("id_5", "200.3,300.3"),
	    	Row.of("id_6", "200.4,300.4"),
	    	Row.of("id_7", "200.5,300.5"),
	    	Row.of("id_8", "200.6,300.6"),
	    	Row.of("id_9", "2.1,3.1"),
	    	Row.of("id_10", "2.1,3.1"),
	    	Row.of("id_11", "2.1,3.1"),
	    	Row.of("id_12", "2.1,3.1"),
	    	Row.of("id_16", "300.,3.2")
		);

		MemSourceBatchOp inputOp = new MemSourceBatchOp(trainData,
			new String[] {"id", "vec"});
		AgnesBatchOp op = new AgnesBatchOp()
			.setIdCol("id")
			.setVectorCol("vec")
			.setPredictionCol("pred")
			.linkFrom(inputOp);
		op.print();
	}
}
```

### 运行结果
id|pred
---|----
id_1|0
id_2|0
id_9|0
id_10|0
id_11|0
id_12|0
id_16|0
id_3|1
id_4|1
id_5|1
id_6|1
id_7|1
id_8|1
