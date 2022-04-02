# FM分类预测 (FmClassifierPredictBatchOp)
Java 类名：com.alibaba.alink.operator.batch.classification.FmClassifierPredictBatchOp

Python 类名：FmClassifierPredictBatchOp


## 功能介绍
FM即因子分解机（Factor Machine），它的特点是考虑了特征之间的相互作用，是一种非线性模型。该组件使用FM模型解决分类问题。

### 算法原理
FM模型是线性模型的升级，是在线性表达式后面加入了新的交叉项特征及对应的权值，FM模型的表达式如下所示：
![](https://img.alicdn.com/imgextra/i1/O1CN01cmatso24OY6CKEvtF_!!6000000007381-2-tps-829-181.png)
这里我们使用 Adagrad 优化算法求解该模型。算法原理细节可以参考文献[1]。

### 算法使用
FM算法是推荐领域被验证的效果较好的推荐方案之一，在电商、广告、视频、信息流、游戏的推荐领域有广泛应用。

### 文献
[1] S. Rendle, "Factorization Machines," 2010 IEEE International Conference on Data Mining, 2010, pp. 995-1000, doi: 10.1109/ICDM.2010.127.

## 参数说明


| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| predictionCol | 预测结果列名 | 预测结果列名 | String | ✓ |  |
| predictionDetailCol | 预测详细信息列名 | 预测详细信息列名 | String |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |

## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

df = pd.DataFrame([
       ["1:1.1 3:2.0", 1.0],
       ["2:2.1 10:3.1", 1.0],
       ["1:1.2 5:3.2", 0.0],
       ["3:1.2 7:4.2", 0.0]
])

input = BatchOperator.fromDataframe(df, schemaStr='kv string, label double')
dataTest = input
fm = FmClassifierTrainBatchOp().setVectorCol("kv").setLabelCol("label")
model = input.link(fm)

predictor = FmClassifierPredictBatchOp().setPredictionCol("pred")
predictor.linkFrom(model, dataTest).print()
```
### Java 代码
```java
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.classification.FmClassifierPredictBatchOp;
import com.alibaba.alink.operator.batch.classification.FmClassifierTrainBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FmClassifierPredictBatchOpTest {
	@Test
	public void testFmClassifierPredictBatchOp() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of("1:1.1 3:2.0", 1.0),
			Row.of("2:2.1 10:3.1", 1.0),
			Row.of("1:1.2 5:3.2", 0.0),
			Row.of("3:1.2 7:4.2", 0.0)
		);
		BatchOperator <?> input = new MemSourceBatchOp(df, "kv string, label double");
		BatchOperator dataTest = input;
		BatchOperator <?> fm = new FmClassifierTrainBatchOp().setVectorCol("kv").setLabelCol("label");
		BatchOperator model = input.link(fm);
		BatchOperator <?> predictor = new FmClassifierPredictBatchOp().setPredictionCol("pred");
		predictor.linkFrom(model, dataTest).print();
	}

}
```
### 运行结果
kv	| label	| pred
---|----|-------
1:1.1 3:2.0|1.0|1.0
2:2.1 10:3.1|1.0|1.0
1:1.2 5:3.2|0.0|0.0
3:1.2 7:4.2|0.0|0.0





