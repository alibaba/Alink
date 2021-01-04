# ftrl训练
## 功能介绍
该组件是一个在线学习的组件，支持在线实时训练模型。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| l1 | L1 正则化系数 | L1 正则化系数，默认为0。 | Double |  | 0.0 |
| l2 | 正则化系数 | L2 正则化系数，默认为0。 | Double |  | 0.0 |
| alpha | 希腊字母：阿尔法 | 经常用来表示算法特殊的参数 | Double |  | 0.1 |
| beta | 希腊字母：贝塔 | 经常用来表示算法特殊的参数 | Double |  | 1.0 |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| vectorCol | 向量列名 | 向量列对应的列名，默认值是null | String |  | null |
| vectorSize | 向量长度 | 向量的长度 | Integer | ✓ |  |
| featureCols | 特征列名数组 | 特征列名数组，默认全选 | String[] |  | null |
| withIntercept | 是否有常数项 | 是否有常数项，默认true | Boolean |  | true |
| timeInterval | 时间间隔 | 数据流流动过程中时间的间隔 | Integer |  | 1800 |



## 脚本示例

### 脚本代码
```python
from pyalink.alink import *

trainData0 = RandomTableSourceBatchOp() \
            .setNumCols(5) \
            .setNumRows(100) \
            .setOutputCols(["f0", "f1", "f2", "f3", "label"]) \
            .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)")

model = LogisticRegressionTrainBatchOp() \
            .setFeatureCols(["f0", "f1", "f2", "f3"]) \
            .setLabelCol("label") \
            .setMaxIter(10).linkFrom(trainData0)

trainData1 = RandomTableSourceStreamOp() \
            .setNumCols(5) \
            .setMaxRows(10000) \
            .setOutputCols(["f0", "f1", "f2", "f3", "label"]) \
            .setOutputColConfs("label:weight_set(1.0,1.0,2.0,5.0)") \
            .setTimePerSample(0.1)

models = FtrlTrainStreamOp(model, None) \
            .setFeatureCols(["f0", "f1", "f2", "f3"]) \
            .setLabelCol("label") \
            .setTimeInterval(10) \
            .setAlpha(0.1) \
            .setBeta(0.1) \
            .setL1(0.1) \
            .setL2(0.1)\
            .setVectorSize(4)\
            .setWithIntercept(True) \
            .linkFrom(trainData1).print()
StreamOperator.execute()
```
### 运行结果
```
label	pred	details
1	1	{"1":"0.9999917437501057","2":"8.2562498943117...
1	1	{"1":"0.965917838185468","2":"0.03408216181453...
2	2	{"1":"0.00658782416074899","2":"0.993412175839...
1	1	{"1":"0.9810760570397847","2":"0.0189239429602...
1	1	{"1":"0.9998904582473768","2":"1.0954175262323...
2	2	{"1":"0.00658782416074899","2":"0.993412175839...
1	1	{"1":"0.9999996598523875","2":"3.4014761252088...
2	2	{"1":"2.0589409516880153E-5","2":"0.9999794105...

```

