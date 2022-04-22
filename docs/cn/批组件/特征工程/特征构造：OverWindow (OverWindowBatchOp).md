# 特征构造：OverWindow (OverWindowBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.OverWindowBatchOp

Python 类名：OverWindowBatchOp


## 功能介绍
批式OverWindow特征构造组件。

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |  |
| orderBy | 排序列 | 排序列 | String | ✓ |  |  |
| partitionCols | 分组列名数组 | 分组列名，多列，可选，默认不选 | String[] |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [2, 8, 2.2],
    [2, 7, 3.3],
    [5, 6, 4.4],
    [5, 6, 5.5],
    [7, 5, 6.6],
    [1, 8, 1.1],
    [1, 9, 1.0],
    [7, 5, 7.7],
    [9, 5, 8.8],
    [9, 4, 9.8],
    [19, 4, 8.8]])
data = BatchOperator.fromDataframe(df, schemaStr="f0 bigint, f1 bigint, f2 double")
OverWindowBatchOp().setOrderBy("f0, f1 desc").setClause("count_preceding(*) as cc").linkFrom(data).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.feature.OverWindowBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OverWindowBatchOpTest {
	@Test
	public void testOverWindowBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(2, 8, 2.2),
			Row.of(2, 7, 3.3),
			Row.of(5, 6, 4.4),
			Row.of(5, 6, 5.5),
			Row.of(7, 5, 6.6),
			Row.of(1, 8, 1.1),
			Row.of(1, 9, 1.0),
			Row.of(7, 5, 7.7),
			Row.of(9, 5, 8.8),
			Row.of(9, 4, 9.8)
		);
		BatchOperator <?> data = new MemSourceBatchOp(df, "f0 int, f1 int, f2 double");
		new OverWindowBatchOp().setOrderBy("f0, f1 desc").setClause("count_preceding(*) as cc").linkFrom(data).print();
	}
}
```

### 运行结果

f0|f1|f2|cc
---|---|------|---
1|9|1.0000|0
1|8|1.1000|1
2|8|2.2000|2
2|7|3.3000|3
5|6|4.4000|4
5|6|5.5000|5
7|5|6.6000|6
7|5|7.7000|7
9|5|8.8000|8
9|4|9.8000|9
