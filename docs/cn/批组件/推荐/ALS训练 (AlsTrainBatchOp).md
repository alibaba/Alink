# ALS训练 (AlsTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp

Python 类名：AlsTrainBatchOp


## 功能介绍
ALS (Alternating Lease Square）交替最小二乘法是一种model based的协同过滤算法，
用于对评分矩阵进行因子分解，然后预测user对item的评分。
它通过观察到的所有用户给产品的打分，来推断每个用户的喜好并向用户推荐适合的产品。

### 算法原理
推荐所使用的数据可以抽象成一个[m,n]的矩阵R，R的每一行代表m个用户对所有电影的评分，
n列代表每部电影对应的得分。R是个稀疏矩阵，一个用户只是对所有电影中的一小部分看过，
有评分。通过矩阵分解方法，我可以把这个低秩的矩阵，分解成两个小矩阵的点乘。公式如下：

![](https://img.alicdn.com/imgextra/i4/O1CN01NTXnik1tjCCAaUWCX_!!6000000005937-2-tps-309-61.png)

有了这个矩阵分解公式，我们可以定义代价函数：
![](https://img.alicdn.com/imgextra/i2/O1CN01we0d5921JYxBSXWGk_!!6000000006964-2-tps-560-92.png)

其中：$\lambda$ 为正则项系数。我们需要找出代价函数最小的U和V。常规的梯度下降算法不能求解。但是先固定U求V，再固定V求U，
如此迭代下去，问题就可以解决了。
### 算法使用
ALS算法支持输出item或者user的隐向量，我们可以计算出用户或者物品的相似度，继而进行排序得到用户或者item的top
N相似user或者item。这样在数据进行召回时便可以进行召回了。 比如根据用户用行为的物品召回，当用户浏览了若干了item时，
便将这些item相似的item加入到召回池中，进行排序。

### 参考文献：
1. explicit feedback: Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007
    

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |  |
| rateCol | 打分列列名 | 打分列列名 | String | ✓ | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |  |
| lambda | 正则化系数 | 正则化系数 | Double |  |  | 0.1 |
| nonnegative | 是否约束因子非负 | 是否约束因子非负 | Boolean |  |  | false |
| numBlocks | 分块数目 | 分块数目 | Integer |  |  | 1 |
| numIter | 迭代次数 | 迭代次数，默认为10 | Integer |  |  | 10 |
| rank | 因子数 | 因子数 | Integer |  |  | 10 |




## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    [1, 1, 0.6],
    [2, 2, 0.8],
    [2, 3, 0.6],
    [4, 1, 0.6],
    [4, 2, 0.3],
    [4, 3, 0.4],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')

als = AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
    .setNumIter(10).setRank(10).setLambda(0.01)

model = als.linkFrom(data)
model.print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AlsTrainBatchOpTest {
	@Test
	public void testAlsTrainBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1, 0.6),
			Row.of(2, 2, 0.8),
			Row.of(2, 3, 0.6),
			Row.of(4, 1, 0.6),
			Row.of(4, 2, 0.3),
			Row.of(4, 3, 0.4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int, rating double");
		BatchOperator <?> als = new AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating")
			.setNumIter(10).setRank(10).setLambda(0.01);
		BatchOperator model = als.linkFrom(data);
		model.print();
	}
}
```

### 运行结果

id|p0|p1|p2
---|---|---|---
-1|{"f0":[["user","factors"],["item","factors"],["user","item"]],"f1":[[2,1],[2,1],[2,3]]}|null|null
2|null|1|1
2|null|2|2
2|null|2|3
2|null|4|1
2|null|4|2
2|null|4|3
0|0.15332114696502686 0.2907584309577942 0.08023820072412491 0.12368439137935638 0.16154064238071442 -0.0482657290995121 0.12886907160282135 -0.104159414768219 0.15081298351287842 0.22745263576507568|4|null
1|0.4621395170688629 0.16237080097198486 0.07094596326351166 0.38359203934669495 0.3225877583026886 0.5486094355583191 0.2962109446525574 0.46471306681632996 0.29413706064224243 0.29865172505378723|2|null
1|0.38991987705230713 0.3296423852443695 0.10596991330385208 0.3207378387451172 0.3165116012096405 0.2756109833717346 0.2748037576675415 0.18200615048408508 0.29145893454551697 0.35637563467025757|3|null
0|0.1556907445192337 0.2930602431297302 0.08095365017652512 0.12562905251979828 0.16353283822536469 -0.046881016343832016 0.13057765364646912 -0.10337890684604645 0.15265130996704102 0.2297801822423935|1|null
1|0.3398374617099762 0.6423624157905579 0.17734453082084656 0.2741791605949402 0.35757142305374146 -0.10493437945842743 0.28536728024482727 -0.22857370972633362 0.3338049352169037 0.5030093193054199|1|null
0|0.276202917098999 0.08905492722988129 0.04048972949385643 0.22937875986099243 0.19095991551876068 0.3356474041938782 0.1760021150112152 0.28645196557044983 0.17399948835372925 0.17416398227214813|2|null
