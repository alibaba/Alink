# SQL操作：Where (WhereStreamOp)
Java 类名：com.alibaba.alink.operator.stream.sql.WhereStreamOp

Python 类名：WhereStreamOp


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

URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data = CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data = data.link(WhereStreamOp().setClause("category='Iris-setosa'"))

StreamOperator.execute()

```
### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.operator.stream.sql.WhereStreamOp;
import org.junit.Test;

public class WhereStreamOpTest {
	@Test
	public void testWhereStreamOp() throws Exception {
		String URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		StreamOperator <?> data = new CsvSourceStreamOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		data
			.link(
				new WhereStreamOp().setClause("category='Iris-setosa'")
			)
			.print();

		StreamOperator.execute();
	}
}
```

### 运行结果
sepal_length|sepal_width|petal_length|petal_width|category
------------|-----------|------------|-----------|--------
5.0000|3.2000|1.2000|0.2000|Iris-setosa
5.1000|3.5000|1.4000|0.2000|Iris-setosa
5.4000|3.9000|1.3000|0.4000|Iris-setosa
5.1000|3.7000|1.5000|0.4000|Iris-setosa
5.4000|3.9000|1.7000|0.4000|Iris-setosa
... | ... | ... | ... | ... |
4.4000|2.9000|1.4000|0.2000|Iris-setosa
5.3000|3.7000|1.5000|0.2000|Iris-setosa
4.7000|3.2000|1.6000|0.2000|Iris-setosa
4.9000|3.1000|1.5000|0.1000|Iris-setosa
4.8000|3.1000|1.6000|0.2000|Iris-setosa
