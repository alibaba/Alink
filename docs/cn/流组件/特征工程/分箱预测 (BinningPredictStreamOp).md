# 分箱预测 (BinningPredictStreamOp)
Java 类名：com.alibaba.alink.operator.stream.feature.BinningPredictStreamOp

Python 类名：BinningPredictStreamOp


## 功能介绍

使用等频，等宽或自动分箱的方法对数据进行分箱操作，输入为连续或离散的特征数据，输出为每个特征的分箱规则，在分箱组件中点键右键选择**我要分箱**，可以对分箱结果进行查看和编辑。

该组件为分箱预测流处理组件，在预测时需要指定编码方法，有WOE（直接输出bin的WOE值）、INDEX（输出bin的索引值）、
VECTOR（只有当前bin的索引值非0的向量）、ASSEMBLED_VECTOR（如果是多列，首先转换为VECTOR，输出多列的组合VECTOR）。

### 算法简介
信用评分卡模型在国外是一种成熟的预测方法，尤其在信用风险评估以及金融风险控制领域更是得到了比较广泛的使用，其原理是将模型变量WOE编码方式离散化之后运用logistic回归模型进行建模，其中本文介绍的分箱算法就是这里说的WOE编码过程。

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
| defaultWoe | 默认Woe，在woe为Nan或NULL时替换 | 默认Woe，在woe为Nan或NULL时替换 | Double |  |  | NaN |
| dropLast | 是否删除最后一个元素 | 删除最后一个元素是为了保证线性无关性。默认true | Boolean |  |  | true |
| encode | 编码方法 | 编码方法 | String |  | "WOE", "VECTOR", "ASSEMBLED_VECTOR", "INDEX" | "ASSEMBLED_VECTOR" |
| handleInvalid | 未知token处理策略 | 未知token处理策略。"keep"表示用最大id加1代替, "skip"表示补null， "error"表示抛异常 | String |  | "KEEP", "ERROR", "SKIP" | "KEEP" |
| modelFilePath | 模型的文件路径 | 模型的文件路径 | String |  |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  |  | null |
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
