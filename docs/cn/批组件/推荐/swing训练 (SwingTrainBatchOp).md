# swing训练 (SwingTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.SwingTrainBatchOp

Python 类名：SwingTrainBatchOp


## 功能介绍
Swing 是一种被广泛使用的item召回算法，输入数据只包含user和item即可，使用简单。

Swing算法原理比较简单，是阿里最早使用到的一种召回算法，在阿里多个业务被验证过非常有效的一种召回方式。 它认为 user-item-user 的结构比 itemCF 的单边结构更稳定。

Swing指的是秋千，例如用户u和用户v，都购买过同一件商品i，则三者之间会构成一个类似秋千的关系图。
若用户u和用户v之间除了购买过i外，还购买过商品j，则认为两件商品是具有某种程度上的相似的。
也就是说，商品与商品之间的相似关系，是通过用户关系来传递的。为了衡量物品i和j的相似性，考察都购买了物品i和j的用户u和用户v，
如果这两个用户共同购买的物品越少，则物品i和j的相似性越高。

Swing算法的表达式如下：

用户权重 w_u = 1 / (user_click_num + userAlpha) ^ userBeta

score(item_i, item_j) = sum(wu * wv / (alpha + common_items(wu, wv)))

其中userAlpha、userBeta和alpha，用户可以根据自己的数据情况设置。

此外，可以通过设置maxUserItems和minUserItems，过滤掉关联item太多或者太少的用户信息。
对于出现次数过多的item，设置maxItemNumber参数，当出现次数大于maxItemNumber时，算法从所有的user中随机抽取maxItemNumber个用户，计算结果，因此该算法两次执行的结果不一定相同。

在电商场景中，user和item的关系可以为点击、购买、收藏等。在Feed流场景关系可以为点击、点赞、评论、收藏等，item可以为文章、博主、话题等。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |  |
| alpha | alpha参数 | alpha参数，默认1.0 | Float |  |  | 1.0 |
| maxItemNumber | item参与计算的人数最大值 | 如果item出现次数大于该次数，会随机选择该次数的用户数据，默认1000 | Integer |  |  | 1000 |
| maxUserItems | 用户互动的最大Item数量 | 如果用户互动Item数量大于该次数，该用户数据不参与计算过程，默认1000 | Integer |  |  | 1000 |
| minUserItems | 用户互动的最小Item数量 | 如果用户互动Item数量小于该次数，该用户数据不参与计算过程，默认10 | Integer |  |  | 10 |
| resultNormalize | 结果是否归一化 | 是否归一化，默认False | Boolean |  |  | false |
| userAlpha | 用户alpha参数 | 用户alpha参数，默认5.0, user weight = 1.0/(userAlpha + userClickCount)^userBeta | Float |  |  | 5.0 |
| userBeta | 用户beta参数 | 用户beta参数，默认-0.35, user weight = 1.0/(userAlpha + userClickCount)^userBeta | Float |  |  | -0.35 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ["a1", "11L", 2.2],
    ["a1", "12L", 2.0],
    ["a2", "11L", 2.0],
    ["a2", "12L", 2.0],
    ["a3", "12L", 2.0],
    ["a3", "13L", 2.0],
    ["a4", "13L", 2.0],
    ["a4", "14L", 2.0],
    ["a5", "14L", 2.0],
    ["a5", "15L", 2.0],
    ["a6", "15L", 2.0],
    ["a6", "16L", 2.0],
])

data = BatchOperator.fromDataframe(df_data, schemaStr='user string, item string, rating double')


model = SwingTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setMinUserItems(1)\
    .linkFrom(data)

model.print()

predictor = SwingRecommBatchOp()\
    .setItemCol("item")\
    .setRecommCol("prediction_result")

predictor.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.SwingRecommBatchOp;
import com.alibaba.alink.operator.batch.recommendation.SwingTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SwingTrainBatchOpTest {
	@Test
	public void testSwingTrainBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("a1", "11L", 2.2),
			Row.of("a1", "12L", 2.0),
			Row.of("a2", "11L", 2.0),
			Row.of("a2", "12L", 2.0),
			Row.of("a3", "12L", 2.0),
			Row.of("a3", "13L", 2.0),
			Row.of("a4", "13L", 2.0),
			Row.of("a4", "14L", 2.0),
			Row.of("a5", "14L", 2.0),
			Row.of("a5", "15L", 2.0),
			Row.of("a6", "15L", 2.0),
			Row.of("a6", "16L", 2.0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user string, item string, rating double");
		BatchOperator <?> model = new SwingTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
            .setMinUserItems(1)
			.linkFrom(data);
		model.print();
		BatchOperator <?> predictor = new SwingRecommBatchOp()
			.setItemCol("item")
			.setRecommCol("prediction_result");
		predictor.linkFrom(model, data).print();
	}
}
```

### 运行结果
mainItems|recommItemAndSimilarity
---------|-----------------------
11L|{"object":["12L"],"score":[1.3015095],"itemCol":"item"}
14L|{"object":[],"score":[],"itemCol":"item"}
16L|{"object":[],"score":[],"itemCol":"item"}
15L|{"object":[],"score":[],"itemCol":"item"}
13L|{"object":[],"score":[],"itemCol":"item"}
12L|{"object":["11L"],"score":[1.3015095],"itemCol":"item"}

user|item|rating|prediction_result
----|----|------|-----------------
a1|11L|2.2000|{"item":"[\"12L\"]","score":"[1.3015094995498657]"}
a1|12L|2.0000|{"item":"[\"11L\"]","score":"[1.3015094995498657]"}
a2|11L|2.0000|{"item":"[\"12L\"]","score":"[1.3015094995498657]"}
a2|12L|2.0000|{"item":"[\"11L\"]","score":"[1.3015094995498657]"}
a3|12L|2.0000|{"item":"[\"11L\"]","score":"[1.3015094995498657]"}
a3|13L|2.0000|{"item":"[]","score":"[]"}
a4|13L|2.0000|{"item":"[]","score":"[]"}
a4|14L|2.0000|{"item":"[]","score":"[]"}
a5|14L|2.0000|{"item":"[]","score":"[]"}
a5|15L|2.0000|{"item":"[]","score":"[]"}
a6|15L|2.0000|{"item":"[]","score":"[]"}
a6|16L|2.0000|{"item":"[]","score":"[]"}
