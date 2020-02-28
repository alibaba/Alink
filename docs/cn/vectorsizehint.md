## 功能介绍
取出Vector 的size进行检测，并进行处理。
## 参数说明
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| size | 向量大小 | 用于判断向量的大小是否和设置的一致 | Integer | ✓ |  |
| handleInvalid | 处理无效值的方法 | 处理无效值的方法，可取 error, optimistic | String |  | "error" |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->

## 脚本示例
#### 运行脚本
``` python
data = np.array([["$8$1:3,2:4,4:7"],["$8$2:4,4:5"]])
df = pd.DataFrame({"vec" : data[:,0]})
data = dataframeToOperator(df, schemaStr="vec string",op_type="batch")
model = VectorSizeHint().setSelectedCol("vec").setOutputCol("vec_hint").setHandleInvalidMethod("optimistic").setSize(8)
model.transform(data).collectToDataframe()
```
#### 运行结果
|vec|vec_hint|
|---|--------|
|$8$1:3,2:4,4:7|$8$1:3.0 2:4.0 4:7.0|
|$8$2:4,4:5|$8$2:4.0 4:5.0|
