# 特征哈希 (FeatureHasherStreamOp)
Java 类名：com.alibaba.alink.operator.stream.feature.FeatureHasherStreamOp

Python 类名：FeatureHasherStreamOp


## 功能介绍
将多个特征组合成一个特征向量。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |
| numFeatures | 向量维度 | 生成向量长度 | Integer |  | 262144 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [1.1, True, "2", "A"],
    [1.1, False, "2", "B"],
    [1.1, True, "1", "B"],
    [2.2, True, "1", "A"]
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')

hasher = FeatureHasherBatchOp().setSelectedCols(["double", "bool", "number", "str"]).setOutputCol("output").setNumFeatures(200)
hasher.linkFrom(inOp1).print()

hasher = FeatureHasherStreamOp().setSelectedCols(["double", "bool", "number", "str"]).setOutputCol("output").setNumFeatures(200)
hasher.linkFrom(inOp2).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.FeatureHasherBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.FeatureHasherStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FeatureHasherStreamOpTest {
	@Test
	public void testFeatureHasherStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(1.1, true, 2, "A"),
			Row.of(1.1, false, 2, "B"),
			Row.of(1.1, true, 1, "B"),
			Row.of(2.2, true, 1, "A")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "double double, bool boolean, number int, str string");
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "double double, bool boolean, number int, str string");
		BatchOperator <?> hasher = new FeatureHasherBatchOp().setSelectedCols("double", "bool", "number", "str")
			.setOutputCol("output").setNumFeatures(200);
		hasher.linkFrom(inOp1).print();
		StreamOperator <?> hasher2 = new FeatureHasherStreamOp().setSelectedCols("double", "bool", "number", "str")
			.setOutputCol("output").setNumFeatures(200);
		hasher2.linkFrom(inOp2).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

批预测结果
f_string|f_long|f_double
--------|------|--------
a|2|0
b|2|0
c|4|0
d|0|4
a|2|0
b|2|0
c|4|0
d|0|4

流预测结果

f_string|f_long|f_double
--------|------|--------
c|4|0
a|2|0
d|0|4
d|0|4
b|2|0
c|4|0
a|2|0
b|2|0
