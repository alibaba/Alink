# 特征哈希 (FeatureHasherBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.FeatureHasherBatchOp

Python 类名：FeatureHasherBatchOp


## 功能介绍
将多个特征组合成一个特征向量。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| categoricalCols | 离散特征列名 | 离散特征列名 | String[] |  |  |  |
| numFeatures | 向量维度 | 生成向量长度 | Integer |  |  | 262144 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |


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

public class FeatureHasherBatchOpTest {
	@Test
	public void testFeatureHasherBatchOp() throws Exception {
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

##### 输出数据
double|bool|number|str|output
------|----|------|---|------
1.1000|true|2|A|$200$13:2.0 38:1.1 45:1.0 195:1.0
1.1000|false|2|B|$200$13:2.0 30:1.0 38:1.1 76:1.0
1.1000|true|1|B|$200$13:1.0 38:1.1 76:1.0 195:1.0
2.2000|true|1|A|$200$13:1.0 38:2.2 45:1.0 195:1.0
