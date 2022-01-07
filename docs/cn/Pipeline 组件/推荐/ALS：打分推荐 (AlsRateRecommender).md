# ALS：打分推荐 (AlsRateRecommender)
Java 类名：com.alibaba.alink.pipeline.recommendation.AlsRateRecommender

Python 类名：AlsRateRecommender


## 功能介绍
ALS预测，可进行评分流预测。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemCol | Item列列名 | Item列列名 | String | ✓ |  |
| recommCol | 推荐结果列名 | 推荐结果列名 | String | ✓ |  |
| userCol | User列列名 | User列列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

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

alsRate = AlsRateRecommender().setUserCol("user").setItemCol("item").setRecommCol("rating") \
        .setRecommCol("predicted_rating").setModelData(model)
    
alsRate.transform(data).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.recommendation.AlsRateRecommender;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AlsRateRecommenderTest {
	@Test
	public void testAlsRateRecommender() throws Exception {
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
		AlsRateRecommender alsRate = new AlsRateRecommender().setUserCol("user").setItemCol("item").setRecommCol(
			"rating")
			.setRecommCol("predicted_rating").setModelData(model);
		alsRate.transform(data).print();
	}
}
```

### 运行结果

user|item|rating|predicted_rating
----|----|------|----------------
1|1|0.6000|0.5810
2|2|0.8000|0.7669
2|3|0.6000|0.5809
4|1|0.6000|0.5753
4|2|0.3000|0.2989
4|3|0.4000|0.3833

