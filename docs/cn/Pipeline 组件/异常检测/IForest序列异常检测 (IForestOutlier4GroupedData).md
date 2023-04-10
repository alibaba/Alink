# IForest序列异常检测 (IForestOutlier4GroupedData)
Java 类名：com.alibaba.alink.pipeline.outlier.IForestOutlier4GroupedData

Python 类名：IForestOutlier4GroupedData


## 功能介绍
iForest 可以识别数据中异常点，在异常检测领域有比较好的效果。算法使用 sub-sampling 方法，降低了算法的计算复杂度。

### 文献或出处
1. [Isolation Forest](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf?q=isolation-forest)

## 参数说明

<!-- PARAMETER TABLE -->

## 代码示例

### Python 代码

```python
import pandas as pd
df = pd.DataFrame([
    [1, 1, 10.0],
    [1, 2, 11.0],
    [1, 3, 12.0],
    [1, 4, 13.0],
    [1, 5, 14.0],
    [1, 6, 15.0],
    [1, 7, 16.0],
    [1, 8, 17.0],
    [1, 9, 18.0],
    [1, 10, 19.0]
])

dataOp = BatchOperator.fromDataframe(
    df, schemaStr='group_id int, id int, val double')

IForestOutlier4GroupedData()\
    .setInputMTableCol("data")\
    .setOutputMTableCol("pred")\
    .setFeatureCols(["val"])\
    .setPredictionCol("detect_pred")\
    .transform(
    dataOp.link(
        GroupByBatchOp()
        .setGroupByPredicate("group_id")
        .setSelectClause("group_id, mtable_agg(id, val) as data")
    )
).print()
```

### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import com.alibaba.alink.pipeline.outlier.IForestOutlier4GroupedData;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IForestOutlier4GroupedDataTest {
	@Test
	public void test() throws Exception {
		List <Row> mTableData = Arrays.asList(
			Row.of(1, 1, 10.0),
			Row.of(1, 2, 11.0),
			Row.of(1, 3, 12.0),
			Row.of(1, 4, 13.0),
			Row.of(1, 5, 14.0),
			Row.of(1, 6, 15.0),
			Row.of(1, 7, 16.0),
			Row.of(1, 8, 17.0),
			Row.of(1, 9, 18.0),
			Row.of(1, 10, 19.0)
		);

		MemSourceBatchOp dataOp = new MemSourceBatchOp(mTableData, new String[] {"group_id", "id", "val"});

		new IForestOutlier4GroupedData()
			.setInputMTableCol("data")
			.setOutputMTableCol("pred")
			.setFeatureCols("val")
			.setPredictionCol("detect_pred")
			.transform(
				dataOp.link(
					new GroupByBatchOp()
						.setGroupByPredicate("group_id")
						.setSelectClause("group_id, mtable_agg(id, val) as data")
				)
			)
			.print();
	}
}
```

### 运行结果

group_id|data|pred
--------|----|----
1|MTable(10,2)(id,val)|MTable(10,3)(id,val,detect_pred)
 |1|10.0000           |1|10.0000|false                 
 |2|11.0000           |2|11.0000|false                 
 |3|12.0000           |3|12.0000|false                 
 |4|13.0000           |4|13.0000|false                 
 |5|14.0000           |5|14.0000|false                 