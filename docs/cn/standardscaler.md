## 功能介绍

标准化是对数据进行按正态化处理的组件

## 参数说明

<!-- OLD_TABLE -->
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| withMean | 是否使用均值 | 是否使用均值，默认使用 | Boolean |  | true |
| withStd | 是否使用标准差 | 是否使用标准差，默认使用 | Boolean |  | true |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |<!-- This is the end of auto-generated parameter info -->


## 脚本示例

#### 脚本

```python
data = np.array([
            ["a", 10.0, 100],
            ["b", -2.5, 9],
            ["c", 100.2, 1],
            ["d", -99.9, 100],
            ["a", 1.4, 1],
            ["b", -2.2, 9],
            ["c", 100.9, 1]
])
             
colnames = ["col1", "col2", "col3"]
selectedColNames = ["col2", "col3"]

df = pd.DataFrame({"col1": data[:, 0], "col2": data[:, 1], "col3": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='batch')


sinOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='stream')
                   

model = StandardScaler()\
           .setSelectedCols(selectedColNames)\
           .fit(inOp)

model.transform(inOp).print()

model.transform(sinOp).print()

StreamOperator.execute()

```

#### 结果

```
  col1      col2      col3
0    a -0.078352  1.459581
1    b -0.259243 -0.481449
2    c  1.226961 -0.652089
3    d -1.668749  1.459581
4    a -0.202805 -0.652089
5    b -0.254902 -0.481449
6    c  1.237091 -0.652089
```
