## 功能介绍

提供流式 UDTF 功能。

在Python环境中，可以提供含eval函数的对象或者lambda函数作为UDTF。

## 参数说明

以下为Python脚本中的参数：

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| func | UDTF 函数 | UDTF 函数 | 含eval函数的对象或者lambda函数 | ✓ |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，必选 | String[] | ✓ |  |
| resultTypes | 输出结果列类型 | 输出结果列类型 | String[] | ✓ |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| joinType  | join 类型 | join 类型 | String | | |

## 脚本示例

### 脚本代码

```
source = CsvSourceStreamOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")

udtfOp = UDTFStreamOp()\
    .setFunc(lambda x, y: [ (yield x + 1 + i, y + 2 + i) for i in range(3) ])\
    .setResultTypes(["DOUBLE", "DOUBLE"])\
    .setSelectedCols(['sepal_length', 'sepal_width'])\
    .setOutputCols(['index', 'x'])\
    .setReservedCols(['sepal_length', 'sepal_width'])
res = udtfOp.linkFrom(source)
res.print()

StreamOperator.execute()
```


### 脚本运行结果

```
  sepal_length sepal_width index    x
1          5.2         4.1   6.2  6.1
2          5.2         4.1   7.2  7.1
3          5.2         4.1   8.2  8.1
4          5.5         2.6   6.5  4.6
5          5.5         2.6   7.5  5.6
...        ...         ...   ...  ...
96         5.7         4.4   7.7  7.4
97         5.7         4.4   8.7  8.4
98         6.5         3.0   7.5  5.0
99         6.5         3.0   8.5  6.0
100        6.5         3.0   9.5  7.0
```

