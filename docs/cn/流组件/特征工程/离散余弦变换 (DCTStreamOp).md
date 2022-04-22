# 离散余弦变换 (DCTStreamOp)
Java 类名：com.alibaba.alink.operator.stream.feature.DCTStreamOp

Python 类名：DCTStreamOp


## 功能介绍

对数据进行离散余弦变换。


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| inverse | 是否为逆变换 | 是否为逆变换，false表示正变换，true表示逆变换。默认正变换。 | Boolean |  |  | false |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df_data = pd.DataFrame([
    ["-0.6264538 0.1836433"],
    ["-0.8356286 1.5952808"],
    ["0.3295078 -0.8204684"],
    ["0.4874291 0.7383247"],
    ["0.5757814 -0.3053884"],
    ["1.5117812 0.3898432"],
    ["-0.6212406 -2.2146999"],
    ["11.1249309 9.9550664"],
    ["9.9838097 10.9438362"],
    ["10.8212212 10.5939013"],
    ["10.9189774 10.7821363"],
    ["10.0745650 8.0106483"],
    ["10.6198257 9.9438713"],
    ["9.8442045 8.5292476"],
    ["9.5218499 10.4179416"],
])

data = StreamOperator.fromDataframe(df_data, schemaStr='features string')

dct = DCTStreamOp().setSelectedCol("features").setOutputCol("result")

dct.linkFrom(data).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.DCTStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DCTStreamOpTest {
	@Test
	public void testDCTStreamOp() throws Exception {
		List <Row> df_data = Arrays.asList(
			Row.of("-0.6264538 0.1836433"),
			Row.of("-0.8356286 1.5952808"),
			Row.of("0.3295078 -0.8204684"),
			Row.of("0.4874291 0.7383247"),
			Row.of("0.5757814 -0.3053884"),
			Row.of("1.5117812 0.3898432"),
			Row.of("-0.6212406 -2.2146999"),
			Row.of("11.1249309 9.9550664"),
			Row.of("9.9838097 10.9438362"),
			Row.of("10.8212212 10.5939013"),
			Row.of("10.9189774 10.7821363"),
			Row.of("10.0745650 8.0106483"),
			Row.of("10.6198257 9.9438713"),
			Row.of("9.8442045 8.5292476"),
			Row.of("9.5218499 10.4179416")
		);
		StreamOperator <?> data = new MemSourceStreamOp(df_data, "features string");
		StreamOperator <?> dct = new DCTStreamOp().setSelectedCol("features").setOutputCol("result");
		dct.linkFrom(data).print();
		StreamOperator.execute();
	}
}
```

### 运行结果

features|result
--------|------
0.4874291 0.7383247|0.866738824045179 -0.17740998012986753
0.3295078 -0.8204684|-0.34716156955541605 0.8131559692231375
-0.6264538 0.1836433|-0.31311430733060563 -0.5728251528295567
-0.8356286 1.5952808|0.5371552219632794 -1.7189125211901217
-0.6212406 -2.2146999|-2.005312758591568 1.126745876574769
11.1249309 9.9550664|14.905809038224113 0.8272191210194105
1.5117812 0.3898432|1.3446515085097996 0.7933299678708727
9.9838097 10.9438362|14.798080330160849 -0.6788412482687869
9.8442045 8.5292476|12.991992573716212 0.9298149409580412
10.0745650 8.0106483|12.788176963635138 1.4594094943741613
9.5218499 10.4179416|14.099561785095878 -0.6336325176349823
10.8212212 10.5939013|15.142778339690611 0.1607394427886475
10.9189774 10.7821363|15.345004656570287 0.09676126975502636
0.5757814 -0.3053884|0.19119672388537412 0.6230811409567939
10.6198257 9.9438713|14.54072959496546 0.4779719400128842
