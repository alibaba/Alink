# IForest序列异常检测 (IForestOutlier4GroupedDataBatchOp)
Java 类名：com.alibaba.alink.operator.batch.outlier.IForestOutlier4GroupedDataBatchOp

Python 类名：IForestOutlier4GroupedDataBatchOp


## 功能介绍
iForest 可以识别数据中异常点，在异常检测领域有比较好的效果。算法使用 sub-sampling 方法，降低了算法的计算复杂度。

### 文献或出处
1. [Isolation Forest](https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf?q=isolation-forest)

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| inputMTableCol | Not available! | Not available! | String | ✓ |  |  |
| outputMTableCol | Not available! | Not available! | String | ✓ |  |  |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |  |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| maxOutlierNumPerGroup | 每组最大异常点数目 | 每组最大异常点数目 | Integer |  |  |  |
| maxOutlierRatio | 最大异常点比例 | 算法检测异常点的最大比例 | Double |  |  |  |
| numTrees | 模型中树的棵数 | 模型中树的棵数 | Integer |  |  | 100 |
| outlierThreshold | 异常评分阈值 | 只有评分大于该阈值才会被认为是异常点 | Double |  |  |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |  |
| subsamplingSize | 每棵树的样本采样行数 | 每棵树的样本采样行数，默认 256 ，最小 2 ，最大 100000 . | Integer |  | [1, 100000] | 256 |
| tensorCol | tensor列 | tensor列 | String |  | 所选列类型为 [BOOL_TENSOR, BYTE_TENSOR, DOUBLE_TENSOR, FLOAT_TENSOR, INT_TENSOR, LONG_TENSOR, STRING, STRING_TENSOR, TENSOR, UBYTE_TENSOR] | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

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

outlierOp = dataOp.link(
    GroupByBatchOp()
    .setGroupByPredicate("group_id")
    .setSelectClause("mtable_agg(id, val) as data")
).link(
    IForestOutlier4GroupedDataBatchOp()
    .setInputMTableCol("data")
    .setOutputMTableCol("pred")
    .setFeatureCols(["val"])
    .setPredictionCol("detect_pred")
)

outlierOp.print()
```

### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.outlier.IForestOutlier4GroupedDataBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.GroupByBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IForestOutlier4GroupedDataBatchOpTest {
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

		BatchOperator <?> outlierOp = dataOp.link(
			new GroupByBatchOp()
				.setGroupByPredicate("group_id")
				.setSelectClause("group_id, mtable_agg(id, val) as data")
		).link(
			new IForestOutlier4GroupedDataBatchOp()
				.setInputMTableCol("data")
				.setOutputMTableCol("pred")
				.setFeatureCols("val")
				.setPredictionCol("detect_pred")
		);

		outlierOp.print();
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
