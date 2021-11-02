# 随机生成结构数据源 (RandomTableSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp

Python 类名：RandomTableSourceStreamOp


## 功能介绍
随机生成流式的表数据。
## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| numCols | 输出表列数目 | 输出表中列的数目，整型 | Integer | ✓ |  |
| maxRows | 输出表行数目最大值 | 输出数据流的表的行数目的最大值，整型 | Long | ✓ |  |
| idCol | id 列名 | 列名，若列名非空，表示输出表中包含一个整形序列id列，否则无该列 | String |  | "num" |
| outputColConfs | 列配置信息 | 表示每一列的数据分布配置信息 | String |  | null |
| outputCols | 输出列名数组 | 字符串数组，当参数不设置时，算法自动生成 | String[] |  | null |
| timePerSample | 每条样本流过的时间 | 每两条样本间的时间间隔，单位秒 | Double |  | null |
| timeZones | 每条样本流过的时间区间 | 用来控制样本输出频率的参数，每两条样本的输出间隔在这个区间范围内，单位秒 | Double[] |  | null |


## 代码示例
### Python 代码
```python
from pyalink.alink import *

import pandas as pd

useLocalEnv(1)

# 生成测试数据，该数据符合高斯分布
data = RandomTableSourceStreamOp() \
                .setNumCols(4) \
                .setMaxRows(10) \
                .setIdCol("id") \
                .setOutputCols(["group_id", "f0", "f1", "f2"]) \
                .setOutputColConfs("group_id:weight_set(111.0,1.0,222.0,1.0);f0:gauss(0,2);f1:gauss(0,2);f2:gauss(0,2)");
```
### Java 代码
```java
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.RandomTableSourceStreamOp;
import org.junit.Test;

public class RandomTableSourceStreamOpTest {
	@Test
	public void testRandomTableSourceStreamOp() throws Exception {
		StreamOperator <?> data = new RandomTableSourceStreamOp()
			.setNumCols(4)
			.setMaxRows(10L)
			.setIdCol("id")
			.setOutputCols(new String[] {"group_id", "f0", "f1", "f2"})
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

## 备注
    * outputColConfs 参数的例子：
    
    "f0:uniform_open(1,2,nullper=0.1);f3:uniform(1,2,nullper=0.1);f1:gauss(0,1);f4:weight_set(1.0,1.0,2.0,5.0,nullper=0.1);f2:poisson(0.5, nullper=0.1)"
    
    其中分号分割不同列的信息，每列信息中冒号前面是列名，冒号后的字符串是列的分布类型，小括号中的信息是分布参数，nullper表示的是数据中null值的占比
