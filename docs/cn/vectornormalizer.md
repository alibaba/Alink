# Vector Normalize 组件

## 功能介绍
对 Vector 进行正则化操作。

## 算法参数

<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| p | 范数的阶 | 范数的阶，默认2 | Double |  | 2.0 |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->


## 脚本示例

#### 运行脚本
``` python
data = np.array([["1:3,2:4,4:7", 1],\
    ["0:3,5:5", 3],\
    ["2:4,4:5", 4]])
df = pd.DataFrame({"vec" : data[:,0], "id" : data[:,1]})
data = dataframeToOperator(df, schemaStr="vec string, id bigint",op_type="batch")
VectorNormalizer().setSelectedCol("vec").setOutputCol("vec_norm").transform(data).collectToDataframe()
```
#### 运行结果


| vec         | id   | vec_norm                                 |
| ----------- | ---- | ---------------------------------------- |
| 1:3,2:4,4:7 | 1    | 1:0.34874291623145787 2:0.46499055497527714 4:0.813733471206735 |
| 0:3,5:5     | 3    | 0:0.5144957554275265 5:0.8574929257125441 |
| 2:4,4:5     | 4    | 2:0.6246950475544243 4:0.7808688094430304 |

