## 功能介绍

该组件提供批式 UDF 功能。

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
class PlusOne(object):
    def eval(self, x):
        return x + 1
    pass

source = CsvSourceBatchOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")

udfOp = UDFBatchOp() \
    .setFunc(PlusOne()) \
    .setResultType("DOUBLE") \
    .setSelectedCols(['sepal_length']) \
    .setOutputCol('sepal_length_t') \
    .setReservedCols(['sepal_width'])
res = udfOp.linkFrom(source)

res.firstN(10).print()
```


### 脚本运行结果

```
   sepal_length_t  sepal_width
0             6.0          3.2
1             7.6          3.0
2             6.4          3.9
3             6.0          2.3
4             6.1          3.5
5             6.0          2.0
6             6.5          3.5
7             7.2          3.4
8             6.6          2.7
9             7.8          2.8
```
