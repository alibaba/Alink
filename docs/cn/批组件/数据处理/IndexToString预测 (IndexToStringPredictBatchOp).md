# IndexToString预测 (IndexToStringPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.IndexToStringPredictBatchOp

Python 类名：IndexToStringPredictBatchOp


## 功能介绍
基于StringIndexer模型，将一列整数映射为字符串。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| modelName | 模型名字 | 模型名字 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["football"],
    ["football"],
    ["football"],
    ["basketball"],
    ["basketball"],
    ["tennis"],
])


data = BatchOperator.fromDataframe(df, schemaStr='f0 string')

stringIndexer = StringIndexer() \
    .setModelName("string_indexer_model") \
    .setSelectedCol("f0") \
    .setOutputCol("f0_indexed") \
    .setStringOrderType("frequency_asc")

indexed = stringIndexer.fit(data).transform(data)

indexed.print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.dataproc.StringIndexer;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IndexToStringPredictBatchOpTest {
	@Test
	public void testIndexToStringPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("football"),
			Row.of("football"),
			Row.of("football"),
			Row.of("basketball"),
			Row.of("basketball"),
			Row.of("tennis")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string");
		StringIndexer stringIndexer = new StringIndexer()
			.setModelName("string_indexer_model")
			.setSelectedCol("f0")
			.setOutputCol("f0_indexed")
			.setStringOrderType("frequency_asc");
		stringIndexer.fit(data).transform(data).print();
	}
}
```

### 运行结果


f0|f0_indexed|f0_indxed_unindexed
---|----------|-------------------
football|2|football
football|2|football
football|2|football
basketball|1|basketball
basketball|1|basketball
tennis|0|tennis
