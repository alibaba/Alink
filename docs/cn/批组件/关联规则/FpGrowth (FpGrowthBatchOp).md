# FpGrowth (FpGrowthBatchOp)
Java 类名：com.alibaba.alink.operator.batch.associationrule.FpGrowthBatchOp

Python 类名：FpGrowthBatchOp


## 功能介绍
FP Growth(Frequent Pattern growth)算法是一种非时序的关联分析算法. 它利用FP tree生成频繁项集和规则,效率优于传统的Apriori算法。

该算法只需要扫描数据2次。其中第1次扫描获得当个项目的频率，去掉不符合支持度要求的项，并对剩下的项按照置信度降序排序。第2遍扫描是建立一棵频繁项树FP-Tree。

算法生成频繁项集分为两个过程：（1）构建FP树；(2)从FP树中挖掘频繁项集。

论文：Han et al., 《Mining frequent patterns without candidate generation》

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| itemsCol | 项集列名 | 项集列名 | String | ✓ | 所选列类型为 [STRING] |  |
| maxConsequentLength | 最大关联规则后继长度 | 最大关联规则后继(consequent)长度 | Integer |  |  | 1 |
| maxPatternLength | 最大频繁项集长度 | 最大频繁项集长度 | Integer |  |  | 10 |
| minConfidence | 最小置信度 | 最小置信度，同时包含X和Y的样本与包含X的样本之比，反映了当样本中包含项集X时，项集Y同时出现的概率。 | Double |  |  | 0.05 |
| minLift | 最小提升度 | 最小提升度，提升度是用来衡量A出现的情况下，是否会对B出现的概率有所提升。 | Double |  |  | 1.0 |
| minSupportCount | 最小支持度数目 | 最小支持度目，当取值大于或等于0时起作用，当小于0时参数minSupportPercent起作用 | Integer |  |  | -1 |
| minSupportPercent | 最小支持度占比 | 最小支持度占比，当minSupportCount取值小于0时起作用，当minSupportCount大于或等于0时该参数不起作用 | Double |  |  | 0.02 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["A,B,C,D"],
    ["B,C,E"],
    ["A,B,C,E"],
    ["B,D,E"],
    ["A,B,C,D"],
])

data = BatchOperator.fromDataframe(df, schemaStr='items string')

fpGrowth = FpGrowthBatchOp() \
    .setItemsCol("items") \
    .setMinSupportPercent(0.4) \
    .setMinConfidence(0.6)

fpGrowth.linkFrom(data)

fpGrowth.print()
fpGrowth.getSideOutput(0).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.associationrule.FpGrowthBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FpGrowthBatchOpTest {
	@Test
	public void testFpGrowthBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("A,B,C,D"),
			Row.of("B,C,E"),
			Row.of("A,B,C,E"),
			Row.of("B,D,E"),
			Row.of("A,B,C,D")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "items string");
		BatchOperator <?> fpGrowth = new FpGrowthBatchOp()
			.setItemsCol("items")
			.setMinSupportPercent(0.4)
			.setMinConfidence(0.6);
		fpGrowth.linkFrom(data);
		fpGrowth.print();
		fpGrowth.getSideOutput(0).print();
	}
}
```

### 运行结果

频繁项集输出：

itemset|supportcount|itemcount
-------|------------|---------
E|3|1
B,E|3|2
C,E|2|2
B,C,E|2|3
D|3|1
B,D|3|2
C,D|2|2
B,C,D|2|3
A,D|2|2
B,A,D|2|3
C,A,D|2|3
B,C,A,D|2|4
A|3|1
B,A|3|2
C,A|3|2
B,C,A|3|3
C|4|1
B,C|4|2
B|5|1

关联规则输出：

rule|itemcount|lift|support_percent|confidence_percent|transaction_count
----|---------|----|---------------|------------------|-----------------
D=>B|2|1.0000|0.6000|1.0000|3
D=>A|2|1.1111|0.4000|0.6667|2
C,D=>B|3|1.0000|0.4000|1.0000|2
A,D=>B|3|1.0000|0.4000|1.0000|2
B,D=>A|3|1.1111|0.4000|0.6667|2
A,D=>C|3|1.2500|0.4000|1.0000|2
C,D=>A|3|1.6667|0.4000|1.0000|2
C,A,D=>B|4|1.0000|0.4000|1.0000|2
B,A,D=>C|4|1.2500|0.4000|1.0000|2
B,C,D=>A|4|1.6667|0.4000|1.0000|2
C=>A|2|1.2500|0.6000|0.7500|3
C=>B|2|1.0000|0.8000|1.0000|4
B,C=>A|3|1.2500|0.6000|0.7500|3
E=>B|2|1.0000|0.6000|1.0000|3
C,E=>B|3|1.0000|0.4000|1.0000|2
B=>E|2|1.0000|0.6000|0.6000|3
B=>D|2|1.0000|0.6000|0.6000|3
B=>A|2|1.0000|0.6000|0.6000|3
B=>C|2|1.0000|0.8000|0.8000|4
A=>D|2|1.1111|0.4000|0.6667|2
A=>B|2|1.0000|0.6000|1.0000|3
A=>C|2|1.2500|0.6000|1.0000|3
B,A=>D|3|1.1111|0.4000|0.6667|2
C,A=>D|3|1.1111|0.4000|0.6667|2
C,A=>B|3|1.0000|0.6000|1.0000|3
B,A=>C|3|1.2500|0.6000|1.0000|3
B,C,A=>D|4|1.1111|0.4000|0.6667|2
