# 分词 (SegmentStreamOp)
Java 类名：com.alibaba.alink.operator.stream.nlp.SegmentStreamOp

Python 类名：SegmentStreamOp


## 功能介绍

对指定列对应的文章内容进行分词，分词后的各个词语间以空格作为分隔符。用户自定义分词 从参数列表中输入。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| userDefinedDict | 用户自定义字典 | 用户自定义字典 | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, u'二手旧书:医学电磁成像'],
    [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
    [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
    [3, u'二手中国糖尿病文献索引'],
    [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']
])

inOp = StreamOperator.fromDataframe(df, schemaStr='id long, text string')

SegmentStreamOp()\
    .setSelectedCol("text")\
    .setOutputCol("segment")\
    .linkFrom(inOp).print()

StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.SegmentStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SegmentStreamOpTest {
	@Test
	public void testSegmentStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "二手旧书:医学电磁成像"),
			Row.of(1, "二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969"),
			Row.of(2, "二手正版图解象棋入门/谢恩思主编/华龄出版社"),
			Row.of(3, "二手中国糖尿病文献索引"),
			Row.of(4, "二手郁达夫文集（ 国内版 ）全十二册馆藏书")
		);
		StreamOperator <?> inOp = new MemSourceStreamOp(df, "id long, text string");
		new SegmentStreamOp()
			.setSelectedCol("text")
			.setOutputCol("segment")
			.linkFrom(inOp).print();
		StreamOperator.execute();
	}
}
```

### 运行结果
id|text|segment
---|----|-------
0|二手旧书:医学电磁成像|二手 旧书 : 医学 电磁 成像
1|二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969|二手 美国 文学 选读 （   下册   ） 李宜燮 南开大学 出版社   9787310003969
4|二手郁达夫文集（ 国内版 ）全十二册馆藏书|二手 郁达夫 文集 （   国内 版   ） 全 十二册 馆藏 书
2|二手正版图解象棋入门/谢恩思主编/华龄出版社|二手 正版 图解 象棋 入门 / 谢恩 思 主编 / 华龄 出版社
3|二手中国糖尿病文献索引|二手 中国 糖尿病 文献 索引
