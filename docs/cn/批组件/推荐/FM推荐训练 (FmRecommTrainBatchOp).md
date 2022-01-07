# FM推荐训练 (FmRecommTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.recommendation.FmRecommTrainBatchOp

Python 类名：FmRecommTrainBatchOp


## 功能介绍
Fm 推荐是使用Fm算法在推荐场景的一种扩展，用给定打分数据及user和item的特征信息，训练一个推荐专用的Fm模型，
用于预测user对item的评分、对user推荐itemlist，或者对item推荐userlist。


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| rateCol | 打分列列名 | 打分列列名 | String | ✓ |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| userFeatureCols | 用户特征列名字数组 | 用户特征列名字数组 | String[] |  | [] |
| userCategoricalFeatureCols | 用户离散值列名字数组 | 用户离散值列名字数组 | String[] |  | [] |
| itemFeatureCols | item特征列名字数组 | item特征列名字数组 | String[] |  | [] |
| itemCategoricalFeatureCols | item离散值列名字数组 | item离散值列名字数组 | String[] |  | [] |
| initStdev | 初始化参数的标准差 | 初始化参数的标准差 | Double |  | 0.05 |
| lambda0 | 常数项正则化系数 | 常数项正则化系数 | Double |  | 0.0 |
| lambda1 | 线性项正则化系数 | 线性项正则化系数 | Double |  | 0.0 |
| lambda2 | 二次项正则化系数 | 二次项正则化系数 | Double |  | 0.0 |
| learnRate | 学习率 | 学习率 | Double |  | 0.01 |
| withLinearItem | 是否含有线性项 | 是否含有线性项 | Boolean |  | true |
| numEpochs | epoch数 | epoch数 | Integer |  | 10 |
| numFactor | 因子数 | 因子数 | Integer |  | 10 |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  | true |

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

model = FmRecommTrainBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setNumFactor(20)\
    .setRateCol("rating").linkFrom(data);

predictor = FmRateRecommBatchOp()\
    .setUserCol("user")\
    .setItemCol("item")\
    .setRecommCol("prediction_result");

predictor.linkFrom(model, data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.FmRateRecommBatchOp;
import com.alibaba.alink.operator.batch.recommendation.FmRecommTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FmRecommTrainBatchOpTest {
	@Test
	public void testFmRecommTrainBatchOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of(1, 1, 0.6),
			Row.of(2, 2, 0.8),
			Row.of(2, 3, 0.6),
			Row.of(4, 1, 0.6),
			Row.of(4, 2, 0.3),
			Row.of(4, 3, 0.4)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df_data, "user int, item int, rating double");
		BatchOperator <?> model = new FmRecommTrainBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setNumFactor(20)
			.setRateCol("rating").linkFrom(data);
		BatchOperator <?> predictor = new FmRateRecommBatchOp()
			.setUserCol("user")
			.setItemCol("item")
			.setRecommCol("prediction_result");
		predictor.linkFrom(model, data).print();
	}
}
```

### 运行结果
user|	item|	rating|	prediction_result
----|-----|--- |---
1	|1|	0.6|	0.582958
2	|2|	0.8|	0.576914
2	|3|	0.6|	0.508942
4	|1|	0.6|	0.505525
4	|2|	0.3|	0.372908
4	|3|	0.4|	0.347927
