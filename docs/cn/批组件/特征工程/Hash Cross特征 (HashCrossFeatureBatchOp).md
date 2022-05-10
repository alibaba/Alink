# Hash Cross特征 (HashCrossFeatureBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.HashCrossFeatureBatchOp

Python 类名：HashCrossFeatureBatchOp


## 功能介绍

将选定的离散列组合成单列的向量类型的数据。

### 算法原理

将选定列的数据的字符串形式以逗号为分隔符拼接起来，然后使用 ```murmur3_32``` 函数得到哈希值，并将哈希值通过平移的方式转换至
```[0, 特征数)```之间。

### 使用方式

使用需要设置选取列的列名（```selectCols```）和输出列名（```outputCol```），特征数通过参数 ```numFeatures``` 设置，特征数也是
输出列中向量的长度。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| numFeatures | 向量维度 | 生成向量长度 | Integer |  |  | 262144 |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["1.0", "1.0", 1.0, 1],
    ["1.0", "1.0", 0.0, 1],
    ["1.0", "0.0", 1.0, 1],
    ["1.0", "0.0", 1.0, 1],
    ["2.0", "3.0", None, 0],
    ["2.0", "3.0", 1.0, 0],
    ["0.0", "1.0", 2.0, 0],
    ["0.0", "1.0", 1.0, 0]])
data = BatchOperator.fromDataframe(df, schemaStr="f0 string, f1 string, f2 double, label bigint")
cross = HashCrossFeatureBatchOp().setSelectedCols(['f0', 'f1', 'f2']).setOutputCol('cross').setNumFeatures(4)
print(cross.linkFrom(data))
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.HashCrossFeatureBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HashCrossFeatureBatchOpTest {
	@Test
	public void testHashCrossFeatureBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1.0", "1.0", 1.0, 1),
			Row.of("1.0", "1.0", 0.0, 1),
			Row.of("1.0", "0.0", 1.0, 1),
			Row.of("1.0", "0.0", 1.0, 1),
			Row.of("2.0", "3.0", null, 0),
			Row.of("2.0", "3.0", 1.0, 0),
			Row.of("0.0", "1.0", 2.0, 0)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 string, f1 string, f2 double, label bigint");
		BatchOperator <?> cross = new HashCrossFeatureBatchOp().setSelectedCols("f0", "f1", "f2").setOutputCol("cross")
			.setNumFeatures(4);
		System.out.print(cross.linkFrom(data));
	}
}
```

### 运行结果

f0|f1|f2|label|cross
--|--|--|-----|-----
1.0|1.0|0.0000|1|$36$33:1.0
1.0|1.0|1.0000|1|$36$15:1.0
1.0|0.0|1.0000|1|$36$33:1.0
1.0|0.0|1.0000|1|$36$33:1.0
2.0|3.0|1.0000|0|$36$20:1.0
2.0|3.0|None|0|$36$
0.0|1.0|1.0000|0|$36$28:1.0
0.0|1.0|2.0000|0|$36$33:1.0
