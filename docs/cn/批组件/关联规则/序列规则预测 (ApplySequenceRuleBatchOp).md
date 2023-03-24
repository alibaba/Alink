# 序列规则预测 (ApplySequenceRuleBatchOp)
Java 类名：com.alibaba.alink.operator.batch.associationrule.ApplySequenceRuleBatchOp

Python 类名：ApplySequenceRuleBatchOp


## 功能介绍
- 应用频繁子序列关联规则，计算命中的频繁子序列。
- 模型由PrefixSpanBatchOp训练，通过getSideOutputAssociationRules()获取。

输入说明：一个sequence由多个element组成，element之间用分号分隔；一个element由多个item组成，item间用逗号分隔。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ | 所选列类型为 [STRING] |  |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["a;a,b,c;a,c;d;c,f"],
    ["a,d;c;b,c;a,e"],
    ["e,f;a,b;d,f;c;b"],
    ["e;g;a,f;c;b;c"],
])

data = BatchOperator.fromDataframe(df, schemaStr='sequence string')

prefixSpan = PrefixSpanBatchOp() \
    .setItemsCol("sequence") \
    .setMinSupportCount(3)

prefixSpan.linkFrom(data)

prefixSpan.print()
prefixSpan.getSideOutputAssociationRules().print()

ApplySequenceRuleBatchOp()\
    .setSelectedCol("sequence")\
    .setOutputCol("result")\
    .linkFrom(prefixSpan.getSideOutputAssociationRules(), data)\
    .print()
```

### Java代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ApplySequenceRuleBatchOpTest {
	@Test
	public void testPrefixSpan() throws Exception {
		List <Row> rows = Arrays.asList(
			Row.of("a;a,b,c;a,c;d;c,f"),
			Row.of("a,d;c;b,c;a,e"),
			Row.of("e,f;a,b;d,f;c;b"),
			Row.of("e;g;a,f;c;b;c")
		);

		BatchOperator data = new MemSourceBatchOp(rows, "sequence string");
		PrefixSpanBatchOp prefixSpan = new PrefixSpanBatchOp()
			.setItemsCol("sequence")
			.setMinSupportCount(3);

		prefixSpan.linkFrom(data);

		ApplySequenceRuleBatchOp op = new ApplySequenceRuleBatchOp()
			.setSelectedCol("sequence")
			.setOutputCol("result")
			.linkFrom(prefixSpan.getSideOutputAssociationRules(), data);
		op.print();
	}
}
```

### 运行结果

频繁项集输出：

itemset|supportcount|itemcount
-------|------------|---------
e|3|1
f|3|1
a|4|1
a;c|4|2
a;c;c|3|3
a;c;b|3|3
a;b|4|2
b|4|1
b;c|3|2
c|4|1
c;c|3|2
c;b|3|2
d|3|1
d;c|3|2

关联规则输出：

rule|chain_length|support|confidence|transaction_count
----|------------|-------|----------|-----------------
c=>c|2|0.7500|0.7500|3
c=>b|2|0.7500|0.7500|3
d=>c|2|0.7500|1.0000|3
b=>c|2|0.7500|0.7500|3
a=>c|2|1.0000|1.0000|4
a;c=>c|3|0.7500|0.7500|3
a;c=>b|3|0.7500|0.7500|3
a=>b|2|1.0000|1.0000|4

预测结果输出

sequence|result
--------|------
a;a,b,c;a,c;d;c,f|b
a,d;c;b,c;a,e|c
e,f;a,b;d,f;c;b|c
e;g;a,f;c;b;c|
