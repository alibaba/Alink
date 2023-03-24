# 分箱训练 (BinningTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.feature.BinningTrainBatchOp

Python 类名：BinningTrainBatchOp


## 功能介绍

使用等频，等宽或自动分箱的方法对数据进行分箱操作，输入为连续或离散的特征数据，输出为每个特征的分箱规则。

### 算法简介
分箱通常是指对连续变量进行区间划分，将连续变量划分成几个区间变量，主要目的是为了避免“过拟合”，使评分结果更具有稳健性和预测性。这里有个典型的例子就是：用决策树进行评分看起来效果不错，但往往都会因为”过拟合”，模型无法实际应用。

分箱支持等频、等宽两种方法，输入为连续或离散的特征数据，输出为每个特征的分箱规则，分箱结果支持合并、拆分等。

### 算法原理
#### 分箱方法
##### 等频分箱
等频分箱是对特征数据进行排序，按分位点的方式选取用户指定的N个分位点作为分箱边界，若相邻分位点相同则将两个分箱合并，因此分箱结果中有可能少于用户指定的分箱个数。

##### 等宽分箱
等宽分箱是对特征数据按最大值和最小值等平均分成N份，在每一等份的边界作为分箱的边界。

##### 离散变量分箱
字符串类型变量即为离散变量，离散变量的分箱不使用上述的三种分箱方法，对于离散变量的每一种取值会单独分成一个分箱，仅当某个取值小于用户指定的最小阈值时，该取值会被统一归入ELSE分箱中。

#### 相关公式
WOE的计算公式如下:
$$ WOE_i=ln(\dfrac{py_i}{pn_i}) $$

其中，py_i是这个组中正样本个数占所有正样本个数的比例，pn_i是这个组中负样本个数占所有负样本个数的比例。

IV的计算公式如下：
$$ IV_i=(py_i-pn_i)*WOE_i $$


实际使用时，一般用IV评估变量的重要性，IV越大，变量对模型的影响越大，在单个变量内部，再利用WOE判断每个bin的区分度如何，WOE越大，区分度越大。

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |  |
| binningMethod | 连续特征分箱方法 | 连续特征分箱方法 | String |  | "QUANTILE", "BUCKET" | "QUANTILE" |
| discreteThresholds | 离散个数阈值 | 离散个数阈值，低于该阈值的离散样本将不会单独成一个组别。 | Integer |  |  | -2147483648 |
| discreteThresholdsArray | 离散个数阈值数组 | 离散个数阈值，每一列对应数组中一个元素。 | Integer[] |  |  | null |
| discreteThresholdsMap | 离散分箱离散为ELSE的最小阈值, 形式如col0:3, col1:4 | 离散分箱离散为ELSE的最小阈值，形式如col0:3, col1:4。 | String |  |  | null |
| fromUserDefined | 是否读取用户自定义JSON | 是否读取用户自定义JSON, true则为用户自定义分箱，false则按参数配置分箱。 | Boolean |  |  | false |
| labelCol | 标签列名 | 输入表中的标签列名 | String |  |  | null |
| leftOpen | 是否左开右闭 | 左开右闭为true，左闭右开为false | Boolean |  |  | true |
| numBuckets | quantile个数 | quantile个数，对所有列有效。 | Integer |  |  | 2 |
| numBucketsArray | quantile个数 | quantile个数，每一列对应数组中一个元素。 | Integer[] |  |  | null |
| numBucketsMap | 用户定义的bucket个数，形式如col0:3, col1:4 | 用户定义的bucket个数，形式如col0:3, col1:4 | String |  |  | null |
| positiveLabelValueString | 正样本 | 正样本对应的字符串格式。 | String |  |  | "1" |
| userDefinedBin | 用户定义的bin的json | 用户定义的bin的json | String |  |  |  |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [0, 1.0, True, 0, "A", 1],
    [1, 2.1, False, 2, "B", 1],
    [2, 1.1, True, 3, "C", 1],
    [3, 2.2, True, 1, "E", 0],
    [4, 0.1, True, 2, "A", 0],
    [5, 1.5, False, -4, "D", 1],
    [6, 1.3, True, 1, "B", 0],
    [7, 0.2, True, -1, "A", 1],
])

inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int,f0 double, f1 boolean, f2 int, f3 string, label int')
inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int,f0 double, f1 boolean, f2 int, f3 string, label int')

train = BinningTrainBatchOp()\
    .setSelectedCols(["f0", "f1", "f2", "f3"])\
    .setLabelCol("label")\
    .setPositiveLabelValueString("1")\
    .linkFrom(inOp1)

predict = BinningPredictBatchOp()\
    .setSelectedCols(["f0", "f1", "f2", "f3"])\
    .setEncode("INDEX")\
    .setReservedCols(["id", "label"])\
    .linkFrom(train, inOp1)

predict.lazyPrint(10)
train.print()

predict = BinningPredictStreamOp(train)\
    .setSelectedCols(["f0", "f1", "f2", "f3"])\
    .setEncode("INDEX")\
    .setReservedCols(["id", "label"])\
    .linkFrom(inOp2)
    
predict.print()
StreamOperator.execute()
```

### Java代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.feature.BinningPredictStreamOp;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Unit test for Binning.
 */
public class BinningTrainBatchOpTest {
	
	@Test
	public void test() throws Exception {
		List<Row> list = Arrays.asList(
			Row.of(0, 1.0, true, 0, "A", 1),
			Row.of(1, 2.1, false, 2, "B", 1),
			Row.of(2, 1.1, true, 3, "C", 1),
			Row.of(3, 2.2, true, 1, "E", 0),
			Row.of(4, 0.1, true, 2, "A", 0),
			Row.of(5, 1.5, false, -4, "D", 1),
			Row.of(6, 1.3, true, 1, "B", 0),
			Row.of(7, 0.2, true, -1, "A", 1)
		);

		BatchOperator batchOperator = new MemSourceBatchOp(list, "id int, f0 double, f1 boolean, f2 int, f3 string, label int");
		StreamOperator streamOperator = new MemSourceStreamOp(list, "id int, f0 double, f1 boolean, f2 int, f3 string, label int");

		BatchOperator train = new BinningTrainBatchOp()
			.setSelectedCols("f0", "f1", "f2", "f3")
			.setLabelCol("label")
			.setPositiveLabelValueString("1")
			.linkFrom(batchOperator);

		BatchOperator predict = new BinningPredictBatchOp()
    		.setSelectedCols("f0", "f1", "f2", "f3")
			.setReservedCols("id", "label")
			.setEncode("INDEX")
			.linkFrom(train, batchOperator);

		predict.lazyPrint(10);
		train.print();

		StreamOperator predictStream = new BinningPredictStreamOp(train)
			.setSelectedCols("f0", "f1", "f2", "f3")
			.setEncode("INDEX")
			.setReservedCols("id", "label")
			.linkFrom(streamOperator);

		predictStream.print();
		StreamOperator.execute();
	}
}
```

### 运行结果
##### 模型结果
```
                                  FeatureBordersJson
0  [{"binDivideType":"QUANTILE","featureName":"f0...
1  [{"binDivideType":"QUANTILE","featureName":"f2...
2  [{"binDivideType":"DISCRETE","featureName":"f1...
3  [{"binDivideType":"DISCRETE","featureName":"f3...
```

##### 预测结果
id |f0 |f1 |f2 |f3 |label
---|---|---|---|---|-----
0|0|0|0|0|1
1|1|1|1|1|1
2|0|0|1|2|1
3|1|0|0|4|0
4|0|0|1|0|0
5|1|1|0|3|1
6|0|0|0|1|0
7|0|0|0|0|1
