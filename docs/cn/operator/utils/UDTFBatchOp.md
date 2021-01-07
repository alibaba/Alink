## 功能介绍

提供批式 UDTF 功能。

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
class SplitOp(object):
    def eval(self, *args):
        for index, x in enumerate(args):
            yield index, x
    pass

source = CsvSourceBatchOp()\
    .setSchemaStr("sepal_length double, sepal_width double, petal_length double, petal_width double, category string")\
    .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv")
udtfOp = UDTFBatchOp() \
    .setFunc(SplitOp()) \
    .setResultTypes(["LONG", "DOUBLE"]) \
    .setSelectedCols(['sepal_length', 'sepal_width']) \
    .setOutputCols(['index', 'x'])
udtf_res = udtfOp.linkFrom(source)

udtf_res.firstN(10).print()
```


### 脚本运行结果

```
   index    x
0      0  5.1
1      1  3.5
2      0  5.0
3      1  2.0
4      0  5.0
5      1  3.2
6      0  6.6
7      0  5.1
8      1  3.7
9      0  6.4
```

