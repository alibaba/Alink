# 独热编码 (OneHotEncoder)
Java 类名：com.alibaba.alink.pipeline.feature.OneHotEncoder

Python 类名：OneHotEncoder


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| discreteThresholds | 离散个数阈值 | 离散个数阈值，低于该阈值的离散样本将不会单独成一个组别。 | Integer |  |  | -2147483648 |
| discreteThresholdsArray | 离散个数阈值数组 | 离散个数阈值，每一列对应数组中一个元素。 | Integer[] |  |  | null |
| dropLast | 是否删除最后一个元素 | 删除最后一个元素是为了保证线性无关性。默认true | Boolean |  |  | true |
| encode | 编码方法 | 编码方法 | String |  | "VECTOR", "ASSEMBLED_VECTOR", "INDEX" | "ASSEMBLED_VECTOR" |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP", "ERROR", "SKIP" | "KEEP" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  |  | null |
| overwriteSink | 是否覆写已有数据 | 是否覆写已有数据 | Boolean |  |  | false |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    ["a", 1],
    ["b", 1],
    ["c", 1],
    ["e", 2],
    ["a", 2],
    ["b", 1],
    ["c", 2],
    ["d", 2],
    [None, 1]
])

inOp = BatchOperator.fromDataframe(df, schemaStr='query string, weight long')

# one hot train
one_hot = OneHotEncoder().setSelectedCols(["query"]).setOutputCols(["output"])
one_hot.fit(inOp).transform(inOp).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.feature.OneHotEncoder;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class OneHotEncoderTest {
	@Test
	public void testOneHotEncoder() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("a", 1),
			Row.of("b", 1),
			Row.of("c", 1),
			Row.of("e", 2),
			Row.of("a", 2),
			Row.of("b", 1),
			Row.of("c", 2),
			Row.of("d", 2),
			Row.of(null, 1)
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "query string, weight int");
		OneHotEncoder one_hot = new OneHotEncoder().setSelectedCols("query").setOutputCols("output");
		one_hot.fit(inOp).transform(inOp).print();
	}
}
```

### 运行结果
query|weight|output
-----|------|------
a|1|$5$0:1.0
b|1|$5$1:1.0
c|1|$5$2:1.0
e|2|$5$
a|2|$5$0:1.0
b|1|$5$1:1.0
c|2|$5$2:1.0
d|2|$5$3:1.0
null|1|$5$4:1.0



