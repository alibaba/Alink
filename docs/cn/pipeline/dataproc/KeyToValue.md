# KeyToValue

## 功能介绍
* 将数据替换成map表对应的值

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| mapKeyCol | Not available! | Not available! | String | ✓ |  |
| mapValueCol | Not available! | Not available! | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 脚本示例
### 运行脚本
```python
import numpy as np
import pandas as pd
data = np.array([
                ["0L", "1L", 0.6],
                ["2L", "2L", 0.8],
                ["2L", "4L", 0.6],
                ["3L", "1L", 0.6],
                ["3L", "2L", 0.3],
                ["3L", "4L", 0.4]
        ])
df = pd.DataFrame({"uid": data[:, 0], "iid": data[:, 1], "label": data[:, 2]})

source = dataframeToOperator(df, schemaStr='uid string, iid string, label double', op_type='batch')

mapData = np.array([
                ["0L", 0],
                ["1L", 1],
                ["2L", 2],
                ["3L", 3],
                ["4L", 4]
        ])
mapDf = pd.DataFrame({"keycol": mapData[:, 0], "valcol": mapData[:, 1]})

mapSource = dataframeToOperator(mapDf, schemaStr='keycol string, valcol int', op_type='batch')

keyToValue = KeyToValue()\
    .setSelectedCol("uid")\
    .setMapKeyCol("keycol")\
    .setMapValueCol("valcol")\
    .setModelData(mapSource)

keyToValue.transform(source).print()
```
### 运行结果
uid|	iid|	label
---| --- | --- |
	0|	1L|	0.6|
	2|	2L|	0.8|
	2|	4L|	0.6
	3|	1L|	0.6
	3|	2L|	0.3
	3|	4L|	0.4


