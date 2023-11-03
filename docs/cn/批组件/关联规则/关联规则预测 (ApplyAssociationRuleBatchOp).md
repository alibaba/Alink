# 关联规则预测 (ApplyAssociationRuleBatchOp)
Java 类名：com.alibaba.alink.operator.batch.associationrule.ApplyAssociationRuleBatchOp

Python 类名：ApplyAssociationRuleBatchOp


## 功能介绍
- 应用关联规则，计算命中的关联规则的输出。
- 模型由FpGrowthBatchOp训练得出，通过getSideOutputAssociationRules()函数获取模型

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

ApplyAssociationRuleBatchOp()\
    .setSelectedCol("items") \
    .setOutputCol("result") \
    .linkFrom(fpGrowth.getSideOutput(0), data).print()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ApplyAssociationRuleBatchOpTest {
	@Test
	public void testFpGrowth() throws Exception {
		List <Row> rows = Arrays.asList(
			Row.of("A,B,C,D"),
			Row.of("B,C,E"),
			Row.of("A,B,C,E"),
			Row.of("B,D,E"),
			Row.of("A,B,C,D")
			);

		BatchOperator data = new MemSourceBatchOp(rows, "items string");

		FpGrowthBatchOp fpGrowth = new FpGrowthBatchOp()
			.setItemsCol("items")
			.setMinSupportPercent(0.4)
			.setMinConfidence(0.6);

		fpGrowth.linkFrom(data);
		fpGrowth.print();
		fpGrowth.getSideOutputAssociationRules().print();

		ApplyAssociationRuleBatchOp op = new ApplyAssociationRuleBatchOp()
			.setSelectedCol("items")
			.setOutputCol("result")
			.linkFrom(fpGrowth.getSideOutputAssociationRules(), data);
		op.print();
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

关联规则预测输出

items | result
---|---
A,B,C,D|E
B,C,E|A,D
A,B,C,E|D
B,D,E|A,C
A,B,C,D|E

