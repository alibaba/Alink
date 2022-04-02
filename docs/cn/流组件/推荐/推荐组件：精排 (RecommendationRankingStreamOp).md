# 推荐组件：精排 (RecommendationRankingStreamOp)
Java 类名：com.alibaba.alink.operator.stream.recommendation.RecommendationRankingStreamOp

Python 类名：RecommendationRankingStreamOp


## 功能介绍
该组件功能是对召回的结果进行排序，并输出排序后的TopK个object，此处排序算法用户可以通过创建PipelineModel的方式定制，具体使用方式参见代码示例。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| mTableCol | Not available! | Not available! | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| topN | 前N的数据 | 挑选最近的N个数据 | Integer |  | 10 |
| rankingCol | 用来排序的得分列 | 用来排序的得分列 | String |  | null |
| modelStreamFilePath | 模型流的文件路径 | 模型流的文件路径 | String |  | null |
| modelStreamScanInterval | 扫描模型路径的时间间隔 | 描模型路径的时间间隔，单位秒 | Integer |  | 10 |
| modelStreamStartTime | 模型流的起始时间 | 模型流的起始时间。默认从当前时刻开始读。使用yyyy-mm-dd hh:mm:ss.fffffffff格式，详见Timestamp.valueOf(String s) | String |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

import pandas as pd

data = pd.DataFrame([["u6", "0.0 1.0", 0.0, 1.0, 1, "{\"data\":{\"iid\":[18,19,88]},\"schema\":\"iid INT\"}"]])
predData = StreamOperator.fromDataframe(data, schemaStr='uid string, uf string, f0 double, f1 double, labels int, ilist string')
predData = predData.link(ToMTableStreamOp().setSelectedCol("ilist"))
data = pd.DataFrame([
            ["u0", "1.0 1.0", 1.0, 1.0, 1, 18],
			["u1", "1.0 1.0", 1.0, 1.0, 0, 19],
			["u2", "1.0 0.0", 1.0, 0.0, 1, 88],
			["u3", "1.0 0.0", 1.0, 0.0, 0, 18],
			["u4", "0.0 1.0", 0.0, 1.0, 1, 88],
			["u5", "0.0 1.0", 0.0, 1.0, 0, 19],
			["u6", "0.0 1.0", 0.0, 1.0, 1, 88]]);
trainData = BatchOperator.fromDataframe(data, schemaStr='uid string, uf string, f0 double, f1 double, labels int, iid string')
oneHotCols = ["uid", "f0", "f1", "iid"]
multiHotCols = ["uf"]
pipe = Pipeline() \
    .add( \
        OneHotEncoder() \
            .setSelectedCols(oneHotCols) \
			.setOutputCols(["ovec"])) \
    .add( \
		MultiHotEncoder().setDelimiter(" ") \
            .setSelectedCols(multiHotCols) \
            .setOutputCols(["mvec"])) \
	.add( \
		VectorAssembler() \
			.setSelectedCols(["ovec", "mvec"]) \
			.setOutputCol("vec")) \
	.add(
		LogisticRegression() \
            .setVectorCol("vec") \
			.setLabelCol("labels") \
			.setReservedCols(["uid", "iid"]) \
			.setPredictionDetailCol("detail") \
			.setPredictionCol("pred")) \
	.add( \
		JsonValue() \
			.setSelectedCol("detail") \
			.setJsonPath(["$.1"]) \
			.setOutputCols(["score"]))
lrModel = pipe.fit(trainData)
rank = RecommendationRankingStreamOp(lrModel.save())\
			.setMTableCol("ilist")\
			.setOutputCol("il")\
			.setTopN(2)\
			.setRankingCol("score")\
			.setReservedCols(["uid", "labels"])
rank.linkFrom(predData).print()
StreamOperator.execute()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.StreamOperator;
import com.alibaba.alink.operator.batch.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.classification.LogisticRegression;
import com.alibaba.alink.pipeline.dataproc.JsonValue;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;
import com.alibaba.alink.pipeline.feature.MultiHotEncoder;
import com.alibaba.alink.pipeline.feature.OneHotEncoder;
import org.junit.Test;

import java.util.Arrays;

public class RecommendationRankingTest {

	 @Test
	 public void test() throws Exception {

		Row[] predArray = new Row[] {
			Row.of("u6", "0.0 1.0", 0.0, 1.0, 1, "{\"data\":{\"iid\":[18,19,88]},"
				+ "\"schema\":\"iid INT\"}")
		};

		Row[] trainArray = new Row[] {
			Row.of("u0", "1.0 1.0", 1.0, 1.0, 1, 18),
			Row.of("u1", "1.0 1.0", 1.0, 1.0, 0, 19),
			Row.of("u2", "1.0 0.0", 1.0, 0.0, 1, 88),
			Row.of("u3", "1.0 0.0", 1.0, 0.0, 1, 18),
			Row.of("u4", "0.0 1.0", 0.0, 1.0, 1, 88),
			Row.of("u5", "0.0 1.0", 0.0, 1.0, 1, 19),
			Row.of("u6", "0.0 1.0", 0.0, 1.0, 1, 88)
		};
		BatchOperator <?> trainData = new MemSourceBatchOp(Arrays.asList(trainArray),
				new String[] {"uid", "uf", "f0", "f1", "labels", "iid"});
		StreamOperator <?> predData =  new MemSourceStreamOp(Arrays.asList(predArray),
				new String[] {"uid", "uf", "f0", "f1", "labels", "ilist"});

		String[] oneHotCols = new String[] {"uid", "f0", "f1", "iid"};
		String[] multiHotCols = new String[] {"uf"};

		Pipeline pipe = new Pipeline()
			.add(
				new OneHotEncoder()
					.setSelectedCols(oneHotCols)
					.setOutputCols("ovec"))
			.add(
				new MultiHotEncoder().setDelimiter(" ")
					.setSelectedCols(multiHotCols)
					.setOutputCols("mvec"))
			.add(
				new VectorAssembler()
					.setSelectedCols("ovec", "mvec")
					.setOutputCol("vec"))
			.add(
				new LogisticRegression()
					.setVectorCol("vec")
					.setLabelCol("labels")
					.setReservedCols("uid", "iid")
					.setPredictionDetailCol("detail")
					.setPredictionCol("pred"))
			.add(
				new JsonValue()
					.setSelectedCol("detail")
					.setJsonPath("$.1")
					.setOutputCols("score"));
		RecommendationRankingStreamOp rank = new RecommendationRankingStreamOp(pipe.fit(trainData).save())
    			.setMTableCol("ilist")
    			.setOutputCol("ilist")
    			.setTopN(2)
    			.setRankingCol("score")
    			.setReservedCols("uid", "labels");
		rank.linkFrom(predData).print();
    	StreamOperator.execute();
	}
}
```

### 运行结果
uid|uf|f0|f1|labels|ilist
---|---|---|---|------|-----
u6|0.0 1.0|0.0000|1.0000|1|{"data":{"iid":[18,88],"score":[0.9999999999999553,0.9999999999999472]},"schema":"iid INT,score DOUBLE"}
