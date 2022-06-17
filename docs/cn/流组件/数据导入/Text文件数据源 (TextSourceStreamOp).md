# Text文件数据源 (TextSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.TextSourceStreamOp

Python 类名：TextSourceStreamOp


## 功能介绍
按行读取文本文件的数据。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |  |
| ignoreFirstLine | 是否忽略第一行数据 | 是否忽略第一行数据 | Boolean |  |  | false |
| partitions | 分区名 | 1)单级、单个分区示例：ds=20190729；2)多级分区之间用" / "分隔，例如：ds=20190729/dt=12； 3)多个分区之间用","分隔，例如：ds=20190729,ds=20190730 | String |  |  | null |
| textCol | 文本列名称 | 文本列名称 | String |  |  | "text" |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
data = TextSourceStreamOp().setFilePath(URL).setTextCol("text")
data.print()
StreamOperator.execute()
```
### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.TextSourceStreamOp;
import org.junit.Test;

public class TextSourceStreamOpTest {
	@Test
	public void testTextSourceStreamOp() throws Exception {
		String URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		StreamOperator <?> data = new TextSourceStreamOp().setFilePath(URL).setTextCol("text");
		data.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
|text
|----
|5.0,3.2,1.2,0.2,Iris-setosa
|6.6,3.0,4.4,1.4,Iris-versicolor
|5.4,3.9,1.3,0.4,Iris-setosa
|5.0,2.3,3.3,1.0,Iris-versicolor
