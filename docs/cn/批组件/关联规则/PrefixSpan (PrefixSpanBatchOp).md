# PrefixSpan (PrefixSpanBatchOp)
Java 类名：com.alibaba.alink.operator.batch.associationrule.PrefixSpanBatchOp

Python 类名：PrefixSpanBatchOp


## 功能介绍
PrefixSpan算法的全称是Prefix-Projected Pattern Growth，即前缀投影的模式挖掘。

与关联规则挖掘不同的是，频繁序列模式挖掘的对象和结果都是有序的，
即数据集中的项在时间和空间上是有序排列的，输出的结果也是有序的。
比如用户多次购物的购买情况，不同时间点的交易记录就构成了一个购买序列，用户在第一次购买了商品A，第二次购买了商品B和C；那的购物序列<A;B,C>.
当N个用户的购买序列就形成了一个规模为N的数据集。可能会发现存在因果关系的规律。
因此序列模式挖掘相对于关联规则挖掘可以挖掘出更加深刻的知识。

PrefixSpan是从输入序列中选取所有满足支持度的频繁子序列。

算法的目标是挖掘出满足最小支持度的频繁序列。从长度为1的前缀开始挖掘序列模式，搜索对应的投影数据库得到长度为1的前缀对应的频繁序列
，然后递归的挖掘长度为2的前缀所对应的频繁序列，。。。以此类推，一直递归到不能挖掘到更长的前缀挖掘为止。

算法经常用于推荐系统，如电商中的商品推荐、社交媒体中的好友推荐等。

论文《Mining Sequential Patterns by Pattern-Growth: The PrefixSpan Approach》

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| itemsCol | 项集列名 | 项集列名 | String | ✓ | 所选列类型为 [STRING] |  |
| maxPatternLength | 最大频繁项集长度 | 最大频繁项集长度 | Integer |  |  | 10 |
| minConfidence | 最小置信度 | 最小置信度，同时包含X和Y的样本与包含X的样本之比，反映了当样本中包含项集X时，项集Y同时出现的概率。 | Double |  |  | 0.05 |
| minSupportCount | 最小支持度数目 | 最小支持度目，当取值大于或等于0时起作用，当小于0时参数minSupportPercent起作用 | Integer |  |  | -1 |
| minSupportPercent | 最小支持度占比 | 最小支持度占比，当minSupportCount取值小于0时起作用，当minSupportCount大于或等于0时该参数不起作用 | Double |  |  | 0.02 |



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

### 其他输入格式示例
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;

public class PrefixSpanBatchOpTest {
	@Test
	public void testPrefixSpan() throws Exception {
		Row[] rows = new Row[] {
			Row.of("user_a", "1", "2022-03-11 10:12:10", "a"),
			Row.of("user_a", "2", "2022-03-12 10:12:10", "a"),
			Row.of("user_a", "2", "2022-03-12 10:12:10", "b"),
			Row.of("user_a", "2", "2022-03-12 10:12:10", "c"),
			Row.of("user_a", "3", "2022-03-13 10:12:10", "a"),
			Row.of("user_a", "3", "2022-03-13 10:12:10", "c"),
			Row.of("user_a", "4", "2022-03-14 10:12:10", "d"),
			Row.of("user_a", "5", "2022-03-15 10:12:10", "c"),
			Row.of("user_a", "5", "2022-03-15 10:12:10", "f"),
			Row.of("user_b", "1", "2022-03-11 10:12:10", "a"),
			Row.of("user_b", "1", "2022-03-11 10:12:10", "d"),
			Row.of("user_b", "2", "2022-03-12 10:12:10", "c"),
			Row.of("user_b", "3", "2022-03-13 10:12:10", "b"),
			Row.of("user_b", "3", "2022-03-13 10:12:10", "c"),
			Row.of("user_b", "4", "2022-03-14 10:12:10", "a"),
			Row.of("user_b", "4", "2022-03-14 10:12:10", "e"),
			Row.of("user_c", "1", "2022-03-11 10:12:10", "e"),
			Row.of("user_c", "1", "2022-03-11 10:12:10", "f"),
			Row.of("user_c", "2", "2022-03-12 10:12:10", "a"),
			Row.of("user_c", "2", "2022-03-12 10:12:10", "b"),
			Row.of("user_c", "3", "2022-03-13 10:12:10", "d"),
			Row.of("user_c", "3", "2022-03-13 10:12:10", "f"),
			Row.of("user_c", "4", "2022-03-14 10:12:10", "c"),
			Row.of("user_c", "5", "2022-03-15 10:12:10", "b"),
			Row.of("user_d", "1", "2022-03-11 10:12:10", "e"),
			Row.of("user_d", "2", "2022-03-12 10:12:10", "g"),
			Row.of("user_d", "3", "2022-03-13 10:12:10", "a"),
			Row.of("user_d", "3", "2022-03-13 10:12:10", "f"),
			Row.of("user_d", "4", "2022-03-14 10:12:10", "c"),
			Row.of("user_d", "5", "2022-03-15 10:12:10", "b"),
			Row.of("user_d", "6", "2022-03-16 10:12:10", "c")
		};

		BatchOperator op = new MemSourceBatchOp(Arrays.asList(rows), "uid string,order_id string,occur_time string,item string")
			.groupBy("uid,order_id", "uid,order_id,CONCAT_AGG(item) AS items")
			.orderBy("uid,order_id", -1)
			.groupBy("uid", "uid,CONCAT_AGG(items, ';') AS sequence");

		PrefixSpanBatchOp prefixSpan = new PrefixSpanBatchOp()
			.setItemsCol("sequence")
			.setMinSupportCount(3);

		prefixSpan.linkFrom(op).print();
	}
}
```

输入说明：一个sequence由多个element组成，element之间用分号分隔；一个element由多个item组成，item间用逗号分隔。
由于sequence有次序关系，需要保持原有的顺序，因此在中间组合时加入orderBy操作。

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
