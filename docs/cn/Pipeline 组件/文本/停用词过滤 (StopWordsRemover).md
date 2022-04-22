# 停用词过滤 (StopWordsRemover)
Java 类名：com.alibaba.alink.pipeline.nlp.StopWordsRemover

Python 类名：StopWordsRemover


## 功能介绍

过滤文本中的噪声词语（例如：的、是、啊等）。

### 使用方式

文本列通过参数 selectedCol 指定，需要是空格分隔的词语，可以使用分词（Segment）组件的输出结果列。
可以通过参数 caseSensitive 设置过滤时是否大小写敏感。

噪声词表可以从[这里](https://github.com/alibaba/Alink/blob/f69c21cff49518c284727f50e216b23575631dd2/core/src/main/resources/stop.txt)
查看。 通过参数 stopWords 可以添加额外的噪声词语。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |  |
| caseSensitive | 是否大小写敏感 | 大小写敏感 | Boolean |  |  | false |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| stopWords | 用户自定义停用词表 | 用户自定义停用词表 | String[] |  |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  |  | 1 |

## 代码示例

### Python 代码

```python
df = pd.DataFrame([
    [0, u'二手旧书:医学电磁成像'],
    [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
    [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
    [3, u'二手中国糖尿病文献索引'],
    [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id long, text string')

pipeline = Pipeline() \
    .add(Segment().setSelectedCol("text")) \
    .add(StopWordsRemover().setSelectedCol("text"))
pipeline.fit(inOp).transform(inOp).print()
```

### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.nlp.Segment;
import com.alibaba.alink.pipeline.nlp.StopWordsRemover;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class StopWordsRemoverTest {
	@Test
	public void testStopWordsRemover() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "二手旧书:医学电磁成像"),
			Row.of(1, "二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969"),
			Row.of(2, "二手正版图解象棋入门/谢恩思主编/华龄出版社"),
			Row.of(3, "二手中国糖尿病文献索引"),
			Row.of(4, "二手郁达夫文集（ 国内版 ）全十二册馆藏书")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, text string");
		Pipeline pipeline = new Pipeline()
			.add(new Segment().setSelectedCol("text"))
			.add(new StopWordsRemover().setSelectedCol("text"));
		pipeline.fit(inOp).transform(inOp).print();
	}
}
```

### 运行结果

| id  | text                                      |
|-----|:------------------------------------------|
| 0   | 二手 旧书 医学 电磁 成像                            |
| 1   | 二手 美国 文学 选读 下册 李宜燮 南开大学 出版社 9787310003969 |
| 2   | 二手 正版 图解 象棋 入门 谢恩 思 主编 华龄 出版社             |
| 3   | 二手 中国 糖尿病 文献 索引                           |
| 4   | 二手 郁达夫 文集 国内 版 全 十二册 馆藏 书                 |
