# SQL操作：Minus (MinusBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.MinusBatchOp

Python 类名：MinusBatchOp


## 功能介绍
一个数据集减去另一个数据集，去重


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |




## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data1 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data2 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

minusOp = MinusBatchOp()
output = minusOp.linkFrom(data1, data2)
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.MinusBatchOp;
import org.junit.Test;

public class MinusBatchOpTest {
	@Test
	public void testMinusBatchOp() throws Exception {
		String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		BatchOperator <?> data1 = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		BatchOperator <?> data2 = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		BatchOperator <?> minusOp = new MinusBatchOp();
		minusOp.linkFrom(data1, data2).print();
	}
}
```

### 运行结果
sepal_length|sepal_width|petal_length|petal_width|category
------------|-----------|------------|-----------|--------
