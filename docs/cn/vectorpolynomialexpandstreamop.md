# Vector 多项式展开组件

## 功能介绍
对 Vector 进行多项式展开，组成一个新的Vector。

## 算法参数

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| degree | 多项式阶数 | 多项式的阶数，默认2 | Integer |  | 2 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->

## 脚本示例

#### 运行脚本
``` python
data = np.array([["$8$1:3,2:4,4:7"],
    ["$8$2:4,4:5"]])
df = pd.DataFrame({"vec" : data[:,0]})
data = dataframeToOperator(df, schemaStr="vec string",op_type="stream")
VectorPolynomialExpandStreamOp().setSelectedCol("vec").setOutputCol("vec_out").linkFrom(data).print()
StreamOperator.execute()

```
#### 运行结果
| vec            | vec_out                                 |
| -------------- | ---------------------------------------- |
| $8$1:3,2:4,4:7 | $44$2:3.0 4:9.0 5:4.0 7:12.0 8:16.0 14:7.0 16:21.0 17:28.0 19:49.0 |
| $8$2:4,4:5     | $44$5:4.0 8:16.0 14:5.0 17:20.0 19:25.0  |

