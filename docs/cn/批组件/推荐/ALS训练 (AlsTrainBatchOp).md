# ALS训练 (AlsTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp

Python 类名：AlsTrainBatchOp


## 功能介绍
ALS (Alternating Lease Square）交替最小二乘法是一种model based的协同过滤算法，
用于对评分矩阵进行因子分解，然后预测user对item的评分。

参考文献：
1. explicit feedback: Large-scale Parallel Collaborative Filtering for the Netflix Prize, 2007


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| rateCol | 打分列列名 | 打分列列名 | String | ✓ |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| rank | 因子数 | 因子数 | Integer |  | 10 |
| lambda | 正则化系数 | 正则化系数 | Double |  | 0.1 |
| nonnegative | 是否约束因子非负 | 是否约束因子非负 | Boolean |  | false |
| numBlocks | 分块数目 | 分块数目 | Integer |  | 1 |
| numIter | 迭代次数 | 迭代次数，默认为10 | Integer |  | 10 |




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
