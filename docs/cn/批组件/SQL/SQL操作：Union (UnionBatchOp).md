# SQL操作：Union (UnionBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.UnionBatchOp

Python 类名：UnionBatchOp


## 功能介绍
对批式数据进行sql的UNION操作（去重）。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df1 = pd.DataFrame([
    ['Ohio', 2000, 1.5],
    ['Ohio', 2000, 1.5],
    ['Ohio', 2002, 3.6],
    ['Nevada', 2001, 2.4],
    ['Nevada', 2002, 2.9],
    ['Nevada', 2003, 3.2]
])
df2 = pd.DataFrame([
    ['Nevada', 2001, 2.4],
    ['Nevada', 2003, 3.2]
])

batch_data1 = BatchOperator.fromDataframe(df1, schemaStr='f1 string, f2 bigint, f3 double')
batch_data2 = BatchOperator.fromDataframe(df2, schemaStr='f1 string, f2 bigint, f3 double')

UnionBatchOp().linkFrom(batch_data1, batch_data2).print()
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.UnionBatchOp;
import org.junit.Test;

public class UnionBatchOpTest {
    @Test
	public void testUnionBatchOp() throws Exception {
    	List <Row> df1 = Arrays.asList(
    		Row.of("Ohio", 2000, 1.5),
    		Row.of("Ohio", 2000, 1.5),
    		Row.of("Ohio", 2002, 3.6),
    		Row.of("Nevada", 2001, 2.4),
    		Row.of("Nevada", 2002, 2.9),
    		Row.of("Nevada", 2003, 3.2)
    	);
    	List <Row> df2 = Arrays.asList(
    		Row.of("Nevada", 2001, 2.4),
    		Row.of("Nevada", 2003, 3.2)
    	);
    	BatchOperator <?> data1 = new MemSourceBatchOp(df1, "f1 string, f2 int, f3 double");
    	BatchOperator <?> data2 = new MemSourceBatchOp(df2, "f1 string, f2 int, f3 double");
    	BatchOperator <?> union = new UnionBatchOp();
    	union.linkFrom(data1, data2).print();
    }
}
```

### 运行结果
f1|f2|f3
---|---|---
Nevada|2001|2.4000
Nevada|2002|2.9000
Nevada|2003|3.2000
Ohio|2000|1.5000
Ohio|2002|3.6000
