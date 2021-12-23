# SQL操作：OrderBy (OrderByBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.OrderByBatchOp

Python 类名：OrderByBatchOp


## 功能介绍
提供sql的order by语句功能

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| clause | 运算语句 | 运算语句 | String | ✓ |  |
| fetch | fetch的record数目 | fetch的record数目 | Integer |  |  |
| limit | record的limit数 | record的limit数 | Integer |  |  |
| offset | fetch的偏移值 | fetch的偏移值 | Integer |  |  |
| order | 排序方法 | 排序方法 | String |  | "asc" |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data = data.link(OrderByBatchOp().setLimit(10).setClause("sepal_length"))
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.sql.OrderByBatchOp;
import org.junit.Test;

public class OrderByBatchOpTest {
	@Test
	public void testOrderByBatchOp() throws Exception {
		String URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

		BatchOperator <?> data = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		data
			.link(
				new OrderByBatchOp()
					.setLimit(10)
					.setClause("sepal_length")
			)
			.print();
	}
}
```

### 运行结果

sepal_length|sepal_width|petal_length|petal_width|category
------------|-----------|------------|-----------|--------
4.3000|3.0000|1.1000|0.1000|Iris-setosa
4.4000|2.9000|1.4000|0.2000|Iris-setosa
4.4000|3.2000|1.3000|0.2000|Iris-setosa
4.4000|3.0000|1.3000|0.2000|Iris-setosa
4.5000|2.3000|1.3000|0.3000|Iris-setosa
4.6000|3.6000|1.0000|0.2000|Iris-setosa
4.6000|3.2000|1.4000|0.2000|Iris-setosa
4.6000|3.4000|1.4000|0.3000|Iris-setosa
4.6000|3.1000|1.5000|0.2000|Iris-setosa
4.7000|3.2000|1.3000|0.2000|Iris-setosa
