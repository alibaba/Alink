## 功能介绍

提供流式 UDF 功能。

在Python环境中，可以提供含eval函数的对象或者lambda函数作为UDF。

## 参数说明

以下为Python脚本中的参数：

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| func | UDF 函数 | UDF 函数 | 含eval函数的对象或者lambda函数 | ✓ |
| outputCol | 输出结果列列名 | 输出结果列列名 | String | ✓ |  |
| resultType | 输出结果列类型 | 输出结果列类型 | String | ✓ |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |

## 脚本示例

### 脚本代码

```
source = CsvSourceStreamOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")

udfOp = UDFStreamOp() \
    .setFunc(lambda x: x + 1) \
    .setResultType("DOUBLE") \
    .setSelectedCols(['sepal_length']) \
    .setOutputCol('sepal_length_t') \
    .setReservedCols(['sepal_width'])
res = udfOp.linkFrom(source)

res.print()
StreamOperator.execute()
```


### 脚本运行结果

```
     sepal_length_t	sepal_width
1               6.9         3.2
2               6.4         3.7
3               7.9         3.1
4               6.5         2.5
5               6.4         3.4
...             ...         ...
96              8.9         3.8
97              6.2         2.7
98              7.4         2.7
99              7.8         3.0
100	            6.7         2.5
```
