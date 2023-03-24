# 分组FPGrowth训练 (GroupedFpGrowthBatchOp)
Java 类名：com.alibaba.alink.operator.batch.associationrule.GroupedFpGrowthBatchOp

Python 类名：GroupedFpGrowthBatchOp


## 功能介绍

分组FpGrowth组件按照指定的分组列，在每个分组内使用FpGrowth算法进行频繁项集挖掘。

FpGrowth算法详见FpGrowthBatchOp

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| itemsCol | 项集列名 | 项集列名 | String | ✓ | 所选列类型为 [STRING] |  |
| groupCol | 分组单列名 | 分组单列名，可选 | String |  |  | null |
| maxConsequentLength | 最大关联规则后继长度 | 最大关联规则后继(consequent)长度 | Integer |  |  | 1 |
| maxPatternLength | 最大频繁项集长度 | 最大频繁项集长度 | Integer |  |  | 10 |
| minConfidence | 最小置信度 | 最小置信度，同时包含X和Y的样本与包含X的样本之比，反映了当样本中包含项集X时，项集Y同时出现的概率。 | Double |  |  | 0.05 |
| minLift | 最小提升度 | 最小提升度，提升度是用来衡量A出现的情况下，是否会对B出现的概率有所提升。 | Double |  |  | 1.0 |
| minSupportCount | 最小支持度数目 | 最小支持度目，当取值大于或等于0时起作用，当小于0时参数minSupportPercent起作用 | Integer |  |  | -1 |
| minSupportPercent | 最小支持度占比 | 最小支持度占比，当minSupportCount取值小于0时起作用，当minSupportCount大于或等于0时该参数不起作用 | Double |  |  | 0.02 |

<!--
## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["changjiang", "A,B,C,D"],
    ["changjiang", "B,C,E"],
    ["huanghe", "A,B,C,E"],
    ["huanghe", "B,D,E"],
    ["huanghe", "A,B,C,D"],
])

data = BatchOperator.fromDataframe(df, schemaStr='group string, items string')

fpGrowth = GroupedFpGrowthBatchOp() \
    .setItemsCol("items").setGroupCol("group").setMinSupportCount(2)

fpGrowth.linkFrom(data)

fpGrowth.print()
```
### Java代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;

public class GroupedFpGrowthBatchOpTest {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of("changjiang", "A,B,C,D"),
			Row.of("changjiang", "B,C,E"),
			Row.of("huanghe", "A,B,C,E"),
			Row.of("huanghe", "B,D,E"),
			Row.of("huanghe", "A,B,C,D"),
		};

		BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), "group string, items string");

		BatchOperator fpgrowth = new GroupedFpGrowthBatchOp()
			.setGroupCol("group")
			.setItemsCol("items")
			.setMinSupportCount(2);

		fpgrowth.linkFrom(data);
		fpgrowth.print();
	}
}
```
### 三元组输入
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;

public class GroupedFpGrowthBatchOpTest {
	@Test
	public void test() throws Exception {
		Row[] rows = new Row[] {
			Row.of("changjiang", 1, "A"),
			Row.of("changjiang", 1, "B"),
			Row.of("changjiang", 1, "C"),
			Row.of("changjiang", 1, "D"),
			Row.of("changjiang", 2, "B"),
			Row.of("changjiang", 2, "C"),
			Row.of("changjiang", 2, "E"),
			Row.of("huanghe", 3, "A"),
			Row.of("huanghe", 3, "B"),
			Row.of("huanghe", 3, "C"),
			Row.of("huanghe", 3, "E"),
			Row.of("huanghe", 4, "B"),
			Row.of("huanghe", 4, "D"),
			Row.of("huanghe", 4, "E"),
			Row.of("huanghe", 5, "A"),
			Row.of("huanghe", 5, "B"),
			Row.of("huanghe", 5, "C"),
			Row.of("huanghe", 5, "D"),
		};

		BatchOperator data = new MemSourceBatchOp(Arrays.asList(rows), "groupid string, id int, item string")
			.groupBy("groupid,id", "groupid,id,CONCAT_AGG(item) AS items");

		BatchOperator fpgrowth = new GroupedFpGrowthBatchOp()
			.setGroupCol("groupid")
			.setItemsCol("items")
			.setMinSupportCount(2);

		fpgrowth.linkFrom(data);
		fpgrowth.print();
	}
}
```

### 运行结果

group|itemset|supportcount|itemcount
-----|-------|------------|---------
changjiang|B|2|1
changjiang|C|2|1
changjiang|B,C|2|2
huanghe|A|2|1
huanghe|B,A|2|2
huanghe|B|3|1
huanghe|C|2|1
huanghe|B,C|2|2
huanghe|A,C|2|2
huanghe|B,A,C|2|3
huanghe|D|2|1
huanghe|B,D|2|2
huanghe|E|2|1
huanghe|B,E|2|2
