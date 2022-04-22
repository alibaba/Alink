# 随机生成结构数据源 (RandomTableSourceBatchOp)
Java 类名：com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp

Python 类名：RandomTableSourceBatchOp


## 功能介绍
随机生成批式的表数据。
支持5种随机数据生成方法：uniform, uniform_open, gauss, weight_set 和 poisson

### 使用方法
setOutputColConfs(列配置信息参数)的含义和编写方法如下

| 参数示例 | 生成方法 |
| ---- | ---- | 
| uniform(1,2,nullper=0.1) | 1到2闭区间上服从均匀分布，有10%的数据为null的随机数序列 |
| uniform_open(1,2) | 1到2开区间上服从均匀分布的随机数序列 |
| weight_set(1.0,1.0,5.0,2.0) | 由1和5组成的随机序列，生成概率为1：2 |
| gauss(0,1) | 满足均值为0、方差为1的正态分布的随机数序列 |
| poisson(0.5) | 满足lambda为0.5的泊松分布的随机序列|

## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| numCols | 输出表列数目 | 输出表中列的数目，整型 | Integer | ✓ |  |  |
| numRows | 输出表行数目 | 输出表中行的数目，整型 | Long | ✓ |  |  |
| idCol | id 列名 | 列名，若列名非空，表示输出表中包含一个整形序列id列，否则无该列 | String |  |  | null |
| outputColConfs | 列配置信息 | 表示每一列的数据分布配置信息 | String |  |  | null |
| outputCols | 输出列名数组 | 字符串数组，当参数不设置时，算法自动生成 | String[] |  |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

# 生成测试数据，该数据符合高斯分布
data = RandomTableSourceBatchOp() \
                .setNumCols(4) \
                .setNumRows(10) \
                .setIdCol("id") \
                .setOutputCols(["group_id", "f0", "f1", "f2"]) \
                .setOutputColConfs("group_id:weight_set(111.0,1.0,222.0,1.0);f0:gauss(0,2);f1:gauss(0,2);f2:gauss(0,2)")
```
### Java 代码
```java
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.RandomTableSourceBatchOp;
import org.junit.Test;

public class RandomTableSourceBatchOpTest {
	@Test
	public void testRandomTableSourceBatchOp() throws Exception {
		BatchOperator <?> data = new RandomTableSourceBatchOp()
			.setNumCols(4)
			.setNumRows(10L)
			.setIdCol("id")
			.setOutputCols("group_id", "f0", "f1", "f2")
			.setOutputColConfs("group_id:weight_set(111.0,1.0,222.0,1.0);f0:gauss(0,2);f1:gauss(0,2);f2:gauss(0,2)");
	}
}
```
### 运行结果
| group_id | f0 | f1 | f2 |
| --- | --- | --- | --- |
| 0 | -0.9964239045050353 | 0.49718679973825497 | 0.1792735119342329 |
| 1 | -0.6095874515728233 | -0.4127806660140688 | 3.0630804909945755 |
| 2 | 2.213734518739278 | -0.8455555927440792 | -1.600352103528522 |
| 3 | -2.815758921864138 | 0.1660690040206056 | 2.5530930456104337 |
| 4 | 0.4511700080470712 | -0.3189014028331945 | 1.074516449728338 |
| 5 | -0.04526610697025353 | 0.9372115152797922 | 0.8801699948291315 |
| 6 | 1.399940708626108 | 0.094717796828264 | 1.8070419026410982 |
| 7 | -1.9583513162921702 | 2.8640034727735295 | 0.8341853784130754 |
| 8 | -1.507498072024416 | -0.1315003650747711 | -3.695551497151364 |
| 9 | -0.5509621008083586 | -0.5880629518273883 | 1.5237202683647566 |


