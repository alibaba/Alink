# Vector Slice 流算法

## 功能介绍
对于流数据，取出 Vector 中的若干列，组成一个新的Vector。

## 算法参数

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| indices | 需要被提取的索引数组 | 需要被提取的索引数组 | int[] |  | null |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


## 脚本示例

### 运行脚本
```
data = np.array([["1:3,2:4,4:7", 1],
    ["0:3,5:5", 3],
    ["2:4,4:5", 4]])
df = pd.DataFrame({"vec" : data[:,0], "id" : data[:,1]})
data = dataframeToOperator(df, schemaStr="vec string, id bigint",op_type="stream")   
vecSlice = VectorSliceStreamOp().setSelectedCol("vec").setOutputCol("vec_slice").setIndices([1,2,3])
vecSlice.linkFrom(data).print()
StreamOperator.execute()
```
### 运行结果

| vec         | id   | vec_slice      |
| ----------- | ---- | -------------- |
| 1:3,2:4,4:7 | 1    | $3$0:3.0 1:4.0 |
| 0:3,5:5     | 3    | $3$            |
| 2:4,4:5     | 4    | $3$1:4.0       |
