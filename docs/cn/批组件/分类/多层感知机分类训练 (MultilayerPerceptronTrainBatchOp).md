# 多层感知机分类训练 (MultilayerPerceptronTrainBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.MultilayerPerceptronTrainBatchOp

Python 类名：MultilayerPerceptronTrainBatchOp


## 功能介绍
多层感知机（MLP，Multilayer Perceptron）也被称作人工神经网络（ANN，Artificial Neural Network），经常用来进行多分类问题的训练预测。

### 算法原理
多层感知机算法除了输入输出层外，它中间可以有多个隐层，最简单的MLP只含一个隐层，即三层的结构，如下图：

![](https://img.alicdn.com/imgextra/i3/O1CN0197rvPM290ndhiOaJv_!!6000000008006-2-tps-898-925.png)

从上图可以看到，多层感知机层与层之间是全连接的。多层感知机最左边是输入层，中间是隐藏层，最后是输出层。 其中输出层对应的是各个分类标签，输出层
的每一个节点对应每一个标签的出现的概率。

### 算法使用
多层感知机主要用于多分类问题，类似文字识别，语音识别，文本分析等问题。

- 备注 ：该组件训练的时候 FeatureCols 和 VectorCol 是两个互斥参数，只能有一个参数来描述算法的输入特征。

### 文献
[1]Artificial neural networks (the multilayer perceptron)—a review of applications in the atmospheric sciences
   MW Gardner, SR Dorling - Atmospheric environment, 1998 - Elsevier.

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |  |
| layers | 神经网络层大小 | 神经网络层大小 | int[] | ✓ |  |  |
| blockSize | 数据分块大小，默认值64 | 数据分块大小，默认值64 | Integer |  |  | 64 |
| epsilon | 收敛阈值 | 迭代方法的终止判断阈值，默认值为 1.0e-6 | Double |  | [0.0, +inf) | 1.0E-6 |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | 所选列类型为 [BIGDECIMAL, BIGINTEGER, BYTE, DOUBLE, FLOAT, INTEGER, LONG, SHORT] | null |
| initialWeights | 初始权重值 | 初始权重值 | DenseVector |  |  | null |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | [0.0, +inf) | 0.0 |
| l2 | L2 正则化系数 | L2 正则化系数，默认为0。 | Double |  | [0.0, +inf) | 0.0 |
| maxIter | 最大迭代步数 | 最大迭代步数，默认为 100 | Integer |  | [1, +inf) | 100 |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | 所选列类型为 [DENSE_VECTOR, SPARSE_VECTOR, STRING, VECTOR] | null |



## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
    [5,2,3.5,1,'Iris-versicolor'],
    [5.1,3.7,1.5,0.4,'Iris-setosa'],
    [6.4,2.8,5.6,2.2,'Iris-virginica'],
    [6,2.9,4.5,1.5,'Iris-versicolor'],
    [4.9,3,1.4,0.2,'Iris-setosa'],
    [5.7,2.6,3.5,1,'Iris-versicolor'],
    [4.6,3.6,1,0.2,'Iris-setosa'],
    [5.9,3,4.2,1.5,'Iris-versicolor'],
    [6.3,2.8,5.1,1.5,'Iris-virginica'],
    [4.7,3.2,1.3,0.2,'Iris-setosa'],
    [5.1,3.3,1.7,0.5,'Iris-setosa'],
    [5.5,2.4,3.8,1.1,'Iris-versicolor'],
])

data = BatchOperator.fromDataframe(df, schemaStr='sepal_length double, sepal_width double, petal_length double, petal_width double, category string')

mlpc = MultilayerPerceptronTrainBatchOp() \
  .setFeatureCols(["sepal_length", "sepal_width", "petal_length", "petal_width"]) \
  .setLabelCol("category") \
  .setLayers([4, 8, 3]) \
  .setMaxIter(10)

mlpc.linkFrom(data).print()

```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.MultilayerPerceptronTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MultilayerPerceptronTrainBatchOpTest {
	@Test
	public void testMultilayerPerceptronTrainBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(5.0, 2.0, 3.5, 1.0, "Iris-versicolor"),
			Row.of(5.1, 3.7, 1.5, 0.4, "Iris-setosa"),
			Row.of(6.4, 2.8, 5.6, 2.2, "Iris-virginica"),
			Row.of(6.0, 2.9, 4.5, 1.5, "Iris-versicolor"),
			Row.of(4.9, 3.0, 1.4, 0.2, "Iris-setosa"),
			Row.of(5.7, 2.6, 3.5, 1.0, "Iris-versicolor"),
			Row.of(4.6, 3.6, 1.0, 0.2, "Iris-setosa"),
			Row.of(5.9, 3.0, 4.2, 1.5, "Iris-versicolor"),
			Row.of(6.3, 2.8, 5.1, 1.5, "Iris-virginica"),
			Row.of(4.7, 3.2, 1.3, 0.2, "Iris-setosa"),
			Row.of(5.1, 3.3, 1.7, 0.5, "Iris-setosa"),
			Row.of(5.5, 2.4, 3.8, 1.1, "Iris-versicolor")
		);
		BatchOperator <?> data = new MemSourceBatchOp(df,
			"sepal_length double, sepal_width double, petal_length double, petal_width double, category string");
		BatchOperator <?> mlpc = new MultilayerPerceptronTrainBatchOp()
			.setFeatureCols("sepal_length", "sepal_width", "petal_length", "petal_width")
			.setLabelCol("category")
			.setLayers(new int[] {4, 8, 3})
			.setMaxIter(10);
		mlpc.linkFrom(data).print();
	}
}
```

### 运行结果

model_id|model_info|label_value
--------|----------|-----------
0|{"vectorCol":null,"isVectorInput":"false","layers":"[4,8,3]","featureCols":"[\"sepal_length\",\"sepal_width\",\"petal_length\",\"petal_width\"]"}|null
1048576|{"data":[-0.04909618379949584,0.05244036093590636,-0.09152901171897616,-0.11420795863182999,-0.06894451030664975,-0.1087100099392411,-0.03133631214053002,-0.0835949402584971,0.055071430534465664,-0.2485403481499119,0.15536309455046926,0.09084302188294159,0.04313004210598116,0.1209695093671795,-0.021957259899066,0.15140663126761766,-0.22211330003694998,0.27394043753166797,-0.31909591836628204,-0.3926804078556859,-0.22339902550567065,-0.3305208878336618,-0.04045283333278175,-0.3339761474767195,-0.5583940816604526,0.5859341738552403,-0.8312646954439671,-0.9931435348006701,-0.5671740710711975,-0.9271163871362036,-0.14373309378540336,-0.8379153371392181,0.09342862364920979,0.15332133339951912,-0.0161743204242714,0.07096378086387316,-0.06046420251376381,-3.339767304230771E-4,-0.059945429358191755,-0.004153281219841397,-0.14924287831591582,0.5654865568657063,-0.39993147079405034,-0.35006697409726345,0.5247897806290789,-0.15271634536139592,-0.9719266030451653,-0.25591578284481437,1.0821438930714482,-0.7638582313334012,0.27234738648661777,0.6593289186638527,-1.0006929917244516,-0.11253501216090893,1.2342806175062886,-0.27931954537382886,0.7069111841149615,-0.4710468121148771,-0.45215268301983363,0.2293601369058357,0.21492850171931616,-1.4273448936041484,-0.7624190687978162,2.2148322354865386,-0.5637543416400049,0.9093558156718702,-0.245789030804682]}|null
2251799812636672|null|Iris-virginica
2251799812636673|null|Iris-versicolor
2251799812636674|null|Iris-setosa
