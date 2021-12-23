# ALS隐式训练 (AlsImplicitTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.AlsImplicitTrainBatchOp

Python 类名：AlsImplicitTrainBatchOp


## 功能介绍
隐式ALS (Alternating Lease Square）是针对隐式反馈场景的一种因子分解模型
（详情见参考文献）

参考文献：
1. implicit feedback: Collaborative Filtering for Implicit Feedback Datasets, 2008

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| rateCol | 打分列列名 | 打分列列名 | String | ✓ |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| alpha | 隐式偏好模型系数alpha | 隐式偏好模型系数alpha | Double |  | 40.0 |
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

als = AlsImplicitTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
    .setNumIter(10).setRank(10).setLambda(0.01)

model = als.linkFrom(data)
model.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.AlsImplicitTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AlsImplicitTrainBatchOpTest {
	@Test
	public void testAlsImplicitTrainBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1, 0.6),
			Row.of(2, 2, 0.8),
			Row.of(2, 3, 0.6),
			Row.of(4, 1, 0.6),
			Row.of(4, 2, 0.3),
			Row.of(4, 3, 0.4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int, rating double");
		BatchOperator <?> als = new AlsImplicitTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol(
			"rating")
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
1|0.7067459225654602 0.47736793756484985 0.14002709090709686 0.6041285991668701 0.5332542657852173 0.6160406470298767 0.5020460486412048 0.48263704776763916 0.5093641877174377 0.5752177834510803|3|null
0|0.1763894259929657 -0.1366494745016098 0.6774582266807556 -0.4786214232444763 0.4608040153980255 0.4111763834953308 -0.49566400051116943 -0.25755634903907776 -0.17400074005126953 0.17524860799312592|1|null
0|0.2749805450439453 0.14517369866371155 0.15636394917964935 0.13525108993053436 0.25944361090660095 0.2805081903934479 0.0968703031539917 0.12784309685230255 0.15043362975120544 0.22882965207099915|4|null
1|0.7121766805648804 0.30997803807258606 0.5707741379737854 0.1878654509782791 0.7565042972564697 0.7929229140281677 0.09063886851072311 0.23355203866958618 0.3119025230407715 0.6008232831954956|1|null
1|0.7061372399330139 0.4771113395690918 0.1395183950662613 0.603988528251648 0.5325971245765686 0.615354597568512 0.5019887685775757 0.4824497699737549 0.5091073513031006 0.5747033953666687|2|null
0|0.10347702354192734 0.2801591753959656 -0.5076550841331482 0.6058341264724731 -0.19131097197532654 -0.12141165882349014 0.5839534997940063 0.38140058517456055 0.3221108913421631 0.05817580968141556|2|null
