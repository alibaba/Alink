# 分词 (Segment)
Java 类名：com.alibaba.alink.pipeline.nlp.Segment

Python 类名：Segment


## 功能介绍

对文本进行分词，分词后各个词语间用空格分隔。

### 使用方式

文本列通过参数 selectedCol 指定。
词典文件可以从[这里](https://github.com/alibaba/Alink/blob/f69c21cff49518c284727f50e216b23575631dd2/core/src/main/resources/dict.txt)
查看。 通过参数 userDefinedDict 可以添加额外的词语。

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
df = pd.DataFrame([
    [0, u'二手旧书:医学电磁成像'],
    [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
    [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
    [3, u'二手中国糖尿病文献索引'],
    [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']
])

inOp = BatchOperator.fromDataframe(df, schemaStr='id long, text string')

segment = Segment().setSelectedCol("text").setOutputCol("segment")

segment.transform(inOp).print()
```

### Java 代码

```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.nlp.Segment;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SegmentTest {
	@Test
	public void testSegment() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "二手旧书:医学电磁成像"),
			Row.of(1, "二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969"),
			Row.of(2, "二手正版图解象棋入门/谢恩思主编/华龄出版社"),
			Row.of(3, "二手中国糖尿病文献索引"),
			Row.of(4, "二手郁达夫文集（ 国内版 ）全十二册馆藏书")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, text string");
		Segment segment = new Segment().setSelectedCol("text").setOutputCol("segment");
		segment.transform(inOp).print();
	}
}
```

### 运行结果

| id  | text                                   | segment                                       |
|-----|----------------------------------------|-----------------------------------------------|
| 0   | 二手旧书:医学电磁成像                            | 二手 旧书 : 医学 电磁 成像                              |
| 1   | 二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969 | 二手 美国 文学 选读 （ 下册 ） 李宜燮 南开大学 出版社 9787310003969 |
| 2   | 二手正版图解象棋入门/谢恩思主编/华龄出版社                 | 二手 正版 图解 象棋 入门 / 谢恩 思 主编 / 华龄 出版社             |
| 3   | 二手中国糖尿病文献索引                            | 二手 中国 糖尿病 文献 索引                               |
| 4   | 二手郁达夫文集（ 国内版 ）全十二册馆藏书                  | 二手 郁达夫 文集 （ 国内 版 ） 全 十二册 馆藏 书                 |
