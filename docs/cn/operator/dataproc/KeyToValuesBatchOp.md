# KeyToValues

## 功能介绍
* 将数据替换成map表对应的多个值

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| mapKeyCols | Not available! | Not available! | String[] | ✓ |  |
| mapValueCols | Not available! | Not available! | String[] | ✓ |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |



## 脚本示例
### 运行脚本
```python
import numpy as np
import pandas as pd
```
### 运行结果


