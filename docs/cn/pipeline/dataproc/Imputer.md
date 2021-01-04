# 缺失值填充算法

## 功能介绍

把数据中的缺失值补上


## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| strategy | 缺失值填充规则 | 缺失值填充的规则，支持mean，max，min或者value。选择value时，需要读取fillValue的值 | String |  | "MEAN" |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| fillValue | 填充缺失值 | 自定义的填充值。当strategy为value时，读取fillValue的值 | String |  | null |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |
| outputCols | 输出结果列列名数组 | 输出结果列列名数组，可选，默认null | String[] |  | null |



## 脚本示例


```
data = np.array([
            ["a", 10.0, 100],
            ["b", -2.5, 9],
            ["c", 100.2, 1],
            ["d", -99.9, 100],
            ["a", 1.4, 1],
            ["b", -2.2, 9],
            ["c", 100.9, 1],
            [None, None, None]
])
             
colnames = ["col1", "col2", "col3"]
selectedColNames = ["col2", "col3"]

df = pd.DataFrame({"col1": data[:, 0], "col2": data[:, 1], "col3": data[:, 2]})
inOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='batch')
sinOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='stream')
                   

model = Imputer()\
           .setSelectedCols(selectedColNames)\
           .fit(inOp)

model.transform(inOp).print()

model.transform(sinOp).print()

StreamOperator.execute()
```


### 脚本运行结果

| col1  |       col2  | col3  |
|-------|-------------|-------|
|     a |   10.000000 |   100 |
|     b |   -2.500000 |     9 |
|     c |  100.200000 |     1 |
|     d |  -99.900000 |   100 |
|     a |    1.400000 |     1 |
|     b |   -2.200000 |     9 |
|     c |  100.900000 |     1 |
|  null |   15.414286 |    31 |
