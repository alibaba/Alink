# 数据拆分 (SplitBatchOp)
Java 类名：com.alibaba.alink.operator.batch.dataproc.SplitBatchOp

Python 类名：SplitBatchOp


## 功能介绍
将输入数据按比例拆分为两部分。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| fraction | 拆分到左端的数据比例 | 拆分到左端的数据比例 | Double | ✓ |  |
| randomSeed | 随机数种子 | 随机数种子 | Integer |  | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([['Ohio', 2001, 1.7],
                        ['Ohio', 2002, 3.6],
                        ['Nevada', 2001, 2.4],
                        ['Nevada', 2002, 2.9]])

batch_data = BatchOperator.fromDataframe(df_data, schemaStr='f1 string, f2 bigint, f3 double')

spliter = SplitBatchOp().setFraction(0.5)
spliter.linkFrom(batch_data)
spliter.lazyPrint(-1)
spliter.getSideOutput(0).lazyPrint(-1)

BatchOperator.execute()
```

### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SplitBatchOpTest {
	@Test
    public void testSplitBatchOp() throws Exception {
        List <Row> df_data = Arrays.asList(
    	    Row.of("Ohio", 2001, 1.7),
    		Row.of("Ohio", 2002, 3.6),
    		Row.of("Nevada", 2001, 2.4),
    		Row.of("Nevada", 2002, 2.9)
   		);
        BatchOperator <?> batch_data = new MemSourceBatchOp(df_data, "f1 string, f2 int, f3 double");
        BatchOperator <?> spliter = new SplitBatchOp().setFraction(0.5);
        spliter.linkFrom(batch_data);
        spliter.lazyPrint(-1);
        spliter.getSideOutput(0).lazyPrint(-1);
        BatchOperator.execute();
	}
}
```

### 运行结果

f1|f2|f3
---|---|---
Ohio|2001|1.7000
Nevada|2002|2.9000

f1|f2|f3
---|---|---
Ohio|2002|3.6000
Nevada|2001|2.4000
