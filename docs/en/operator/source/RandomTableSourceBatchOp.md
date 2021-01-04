## Description
Generate table with random data.
 We support five random type of each column: uniform, uniform_open, gauss, weight_set and poisson.
 uniform(1,2,nullper=0.1): uniform from 1 to 2 with 0.1 of the data is null;
 uniform_open(1,2): uniform from 1 to 2 in the open space;
 weight_set(1.0,3.0,2.0,5.0): random generate data of 1.0 and 2.0 while the ratio of the two is 1:2;
 gauss(0,1): generate data from gauss(0, 1);
 poisson(0.5): generate data from poisson distribution with lambda = 0.5 .

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| idCol | id col name | String |  | null |
| outputColConfs | output col confs | String |  | null |
| outputCols | output col names | String[] |  | null |
| numCols | num cols | Integer | ✓ |  |
| numRows | num rows | Long | ✓ |  |

## Script Example
### Code
```python
from pyalink.alink import *
# 生成测试数据，该数据符合高斯分布
data = RandomTableSourceBatchOp() \
                .setNumCols(4) \
                .setNumRows(10) \
                .setIdCol("id") \
                .setOutputCols(["group_id", "f0", "f1", "f2"]) \
                .setOutputColConfs("group_id:weight_set(111.0,1.0,222.0,1.0);f0:gauss(0,2);f1:gauss(0,2);f2:gauss(0,2)");
```
### Result
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

+- 备注：outputColConfs 参数的例子："f0:uniform_open(1,2,nullper=0.1);f3:uniform(1,2,nullper=0.1);f1:gauss(0,1);f4:weight_set(1.0,1.0,2.0,5.0,nullper=0.1);f2:poisson(0.5, nullper=0.1)"，其中分号分割不同列的信息，每列信息中冒号前面是列名，冒号后的字符串是列的分布类型，小括号中的信息是分布参数，nullper表示的是数据中null值的占比
+
