# 停用词过滤 (StopWordsRemoverStreamOp)
Java 类名：com.alibaba.alink.operator.stream.nlp.StopWordsRemoverStreamOp

Python 类名：StopWordsRemoverStreamOp


## 功能介绍
停用词过滤，是文本分析中一个预处理方法。它的功能是过滤分词结果中的噪声（例如：的、是、啊等）。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| caseSensitive | 是否大小写敏感 | 大小写敏感 | Boolean |  | false |
| stopWords | 用户自定义停用词表 | 用户自定义停用词表 | String[] |  | null |
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
    [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, text string')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, text string')

segment = SegmentBatchOp().setSelectedCol("text").setOutputCol("segment").linkFrom(inOp1)
remover = StopWordsRemoverBatchOp().setSelectedCol("segment").setOutputCol("remover").linkFrom(segment)
remover.print()

segment2 = SegmentStreamOp().setSelectedCol("text").setOutputCol("segment").linkFrom(inOp2)
remover2 = StopWordsRemoverStreamOp().setSelectedCol("segment").setOutputCol("remover").linkFrom(segment2)
remover2.print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.nlp.SegmentBatchOp;
import com.alibaba.alink.operator.batch.nlp.StopWordsRemoverBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.nlp.SegmentStreamOp;
import com.alibaba.alink.operator.stream.nlp.StopWordsRemoverStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StopWordsRemoverStreamOpTest {
	@Test
	public void testStopWordsRemoverStreamOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "二手旧书:医学电磁成像"),
			Row.of(1, "二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969"),
			Row.of(2, "二手正版图解象棋入门/谢恩思主编/华龄出版社"),
			Row.of(3, "二手中国糖尿病文献索引")
		);
		BatchOperator <?> inOp1 = new MemSourceBatchOp(df, "id int, text string");
		StreamOperator <?> inOp2 = new MemSourceStreamOp(df, "id int, text string");
		BatchOperator <?> segment =
			new SegmentBatchOp().setSelectedCol("text").setOutputCol("segment").linkFrom(inOp1);
		BatchOperator <?> remover = new StopWordsRemoverBatchOp().setSelectedCol("segment").setOutputCol("remover")
			.linkFrom(segment);
		remover.print();
		StreamOperator <?> segment2 = new SegmentStreamOp().setSelectedCol("text").setOutputCol("segment").linkFrom(
			inOp2);
		StreamOperator <?> remover2 = new StopWordsRemoverStreamOp().setSelectedCol("segment").setOutputCol("remover")
			.linkFrom(segment2);
		remover2.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
#### 批运行结果
id|text|segment|remover
---|----|-------|-------
0|二手旧书:医学电磁成像|二手 旧书 : 医学 电磁 成像|二手 旧书 医学 电磁 成像
1|二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969|二手 美国 文学 选读 （   下册   ） 李宜燮 南开大学 出版社   9787310003969|二手 美国 文学 选读 下册 李宜燮 南开大学 出版社 9787310003969
2|二手正版图解象棋入门/谢恩思主编/华龄出版社|二手 正版 图解 象棋 入门 / 谢恩 思 主编 / 华龄 出版社|二手 正版 图解 象棋 入门 谢恩 思 主编 华龄 出版社
3|二手中国糖尿病文献索引|二手 中国 糖尿病 文献 索引|二手 中国 糖尿病 文献 索引

#### 流运行结果
id|text|segment|remover
---|----|-------|-------
0|二手旧书:医学电磁成像|二手 旧书 : 医学 电磁 成像|二手 旧书 医学 电磁 成像
1|二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969|二手 美国 文学 选读 （   下册   ） 李宜燮 南开大学 出版社   9787310003969|二手 美国 文学 选读 下册 李宜燮 南开大学 出版社 9787310003969
3|二手中国糖尿病文献索引|二手 中国 糖尿病 文献 索引|二手 中国 糖尿病 文献 索引
2|二手正版图解象棋入门/谢恩思主编/华龄出版社|二手 正版 图解 象棋 入门 / 谢恩 思 主编 / 华龄 出版社|二手 正版 图解 象棋 入门 谢恩 思 主编 华龄 出版社
