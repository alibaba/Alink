# PrefixSpan (PrefixSpanBatchOp)
Java 类名：com.alibaba.alink.operator.batch.associationrule.PrefixSpanBatchOp

Python 类名：PrefixSpanBatchOp


## 功能介绍
PrefixSpan是从输入序列中选取所有满足支持度的频繁子序列。

算法的目标是挖掘出满足最小支持度的频繁序列。从长度为1的前缀开始挖掘序列模式，搜索对应的投影数据库得到长度为1的前缀对应的频繁序列，然后递归的挖掘长度为2的前缀所对应的频繁序列，。。。以此类推，一直递归到不能挖掘到更长的前缀挖掘为止。

论文《Mining Sequential Patterns by Pattern-Growth: The PrefixSpan Approach》

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| itemsCol | 项集列名 | 项集列名 | String | ✓ |  |
| maxPatternLength | 最大频繁项集长度 | 最大频繁项集长度 | Integer |  | 10 |
| minConfidence | 最小置信度 | 最小置信度，同时包含X和Y的样本与包含X的样本之比，反映了当样本中包含项集X时，项集Y同时出现的概率。 | Double |  | 0.05 |
| minSupportCount | 最小支持度数目 | 最小支持度目，当取值大于或等于0时起作用，当小于0时参数minSupportPercent起作用 | Integer |  | -1 |
| minSupportPercent | 最小支持度占比 | 最小支持度占比，当minSupportCount取值小于0时起作用，当minSupportCount大于或等于0时该参数不起作用 | Double |  | 0.02 |



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
prefixSpan.getSideOutput(0).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.associationrule.PrefixSpanBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PrefixSpanBatchOpTest {
	@Test
	public void testPrefixSpanBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a;a,b,c;a,c;d;c,f"),
			Row.of("a,d;c;b,c;a,e"),
			Row.of("e,f;a,b;d,f;c;b"),
			Row.of("e;g;a,f;c;b;c")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "sequence string");
		BatchOperator <?> prefixSpan = new PrefixSpanBatchOp()
			.setItemsCol("sequence")
			.setMinSupportCount(3);
		prefixSpan.linkFrom(data);
		prefixSpan.print();
		prefixSpan.getSideOutput(0).print();
	}
}
```

输入说明：一个sequence由多个element组成，element之间用分号分隔；一个element由多个item组成，item间用逗号分隔。

### 运行结果

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
