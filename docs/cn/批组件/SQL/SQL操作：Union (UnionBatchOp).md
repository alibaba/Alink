# SQL操作：Union (UnionBatchOp)
Java 类名：com.alibaba.alink.operator.batch.sql.UnionBatchOp

Python 类名：UnionBatchOp


## 功能介绍
提供sql的union 语句功能


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
data1 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
data2 = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

unionOp = UnionBatchOp()
output = unionOp.linkFrom(data1, data2)
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
		String URL = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv";
		String SCHEMA_STR
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		BatchOperator <?> data1 = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		BatchOperator <?> data2 = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
		BatchOperator <?> unionOp = new UnionBatchOp();
		unionOp.linkFrom(data1, data2).print();
	}
}
```

### 运行结果
sepal_length|sepal_width|petal_length|petal_width|category
------------|-----------|------------|-----------|--------
4.4000|2.9000|1.4000|0.2000|Iris-setosa
4.6000|3.4000|1.4000|0.3000|Iris-setosa
4.6000|3.6000|1.0000|0.2000|Iris-setosa
4.7000|3.2000|1.6000|0.2000|Iris-setosa
5.0000|3.5000|1.3000|0.3000|Iris-setosa
... | ... | ... | ...| ... 
6.0000|2.7000|5.1000|1.6000|Iris-versicolor
6.0000|2.9000|4.5000|1.5000|Iris-versicolor
6.1000|2.8000|4.7000|1.2000|Iris-versicolor
6.4000|3.2000|4.5000|1.5000|Iris-versicolor
6.5000|2.8000|4.6000|1.5000|Iris-versicolor
6.8000|3.0000|5.5000|2.1000|Iris-virginica
