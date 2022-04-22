# Text文件读入 (TextSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.TextSourceBatchOp

Python 类名：TextSourceBatchOp


## 功能介绍

按行读取文件数据

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| ignoreFirstLine | 是否忽略第一行数据 | 是否忽略第一行数据 | Boolean |  |  | false |
| textCol | 文本列名称 | 文本列名称 | String |  |  | "text" |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
data = TextSourceBatchOp().setFilePath(URL).setTextCol("text")
data.print()
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TextSourceBatchOp;
import org.junit.Test;

public class TextSourceBatchOpTest {
	@Test
	public void testTextSourceBatchOp() throws Exception {
		String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		BatchOperator <?> data = new TextSourceBatchOp().setFilePath(URL).setTextCol("text");
		data.print();
	}
}


```

### 运行结果

|text
|----
|6.5,2.8,4.6,1.5,Iris-versicolor
|6.1,3.0,4.9,1.8,Iris-virginica
|7.3,2.9,6.3,1.8,Iris-virginica
|5.7,2.8,4.5,1.3,Iris-versicolor
|6.4,2.8,5.6,2.1,Iris-virginica
|6.7,2.5,5.8,1.8,Iris-virginica
