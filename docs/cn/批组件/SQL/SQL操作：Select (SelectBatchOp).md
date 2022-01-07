# SQL操作：Select (SelectBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.SelectBatchOp

Python 类名：SelectBatchOp


## 功能介绍
提供sql的select语句功能

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
data = data.link(SelectBatchOp().setClause("category as label"))
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.SelectBatchOp;
import org.junit.Test;

public class SelectBatchOpTest {
	@Test
	public void testSelectBatchOp() throws Exception {
		String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		BatchOperator <?> data = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		data.link(new SelectBatchOp().setClause("category as label")).print();
	}
}
```

### 运行结果
label|
-----|
Iris-virginica|
Iris-setosa|
Iris-versicolor|
Iris-versicolor|
Iris-setosa|
...|
Iris-virginica|
Iris-versicolor|
Iris-virginica|
Iris-virginica|
Iris-virginica|
