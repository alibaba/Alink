# Vector 缺失值填充训练组件

## 功能介绍
训练Vecotor 缺失值填充组件的模型，输出模型。

## 算法参数
<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| strategy | 缺失值填充规则 | 缺失值填充的规则，支持mean，max，min或者value。选择value时，需要读取fillValue的值 | String |  | "mean" |
| fillValue | 填充缺失值 | 自定义的填充值。当strategy为value时，读取fillValue的值 | String |  | null |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |<!-- This is the end of auto-generated parameter info -->

## 脚本示例

#### 运行脚本
``` python
data = np.array([["1:3,2:4,4:7", 1],\
    ["1:3,2:NaN", 3],\
    ["2:4,4:5", 4]])
df = pd.DataFrame({"vec" : data[:,0], "id" : data[:,1]})
data = dataframeToOperator(df, schemaStr="vec string, id bigint",op_type="batch")
vecFill = VectorImputer().setSelectedCol("vec").setOutputCol("vec1")
vecFill.fit(data).transform(data).collectToDataframe()
```
#### 运行结果


| vec         | id   | vec1              |
| ----------- | ---- | ----------------- |
| 1:3,2:4,4:7 | 1    | 1:3.0 2:4.0 4:7.0 |
| 1:3,2:NaN   | 3    | 1:3.0 2:4.0       |
| 2:4,4:5     | 4    | 2:4.0 4:5.0       |