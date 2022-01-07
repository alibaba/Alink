# SQL操作：Where (WhereBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.WhereBatchOp

Python 类名：WhereBatchOp


## 功能介绍
提供sql的where语句功能

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data = data.link(WhereBatchOp().setClause("category='Iris-setosa'"))
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.WhereBatchOp;
import org.junit.Test;

public class WhereBatchOpTest {
	@Test
	public void testWhereBatchOp() throws Exception {
		String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		BatchOperator <?> data = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		data.link(new WhereBatchOp().setClause("category='Iris-setosa'")).print();
	}
}
```


### 运行结果
sepal_length|sepal_width|petal_length|petal_width|category
------------|-----------|------------|-----------|--------
5.4000|3.7000|1.5000|0.2000|Iris-setosa
5.4000|3.4000|1.5000|0.4000|Iris-setosa
4.8000|3.4000|1.6000|0.2000|Iris-setosa
5.2000|4.1000|1.5000|0.1000|Iris-setosa
4.8000|3.0000|1.4000|0.1000|Iris-setosa
...|...|...|...|...
4.4000|2.9000|1.4000|0.2000|Iris-setosa
4.7000|3.2000|1.6000|0.2000|Iris-setosa
4.9000|3.1000|1.5000|0.1000|Iris-setosa
4.8000|3.1000|1.6000|0.2000|Iris-setosa
5.0000|3.3000|1.4000|0.2000|Iris-setosa
