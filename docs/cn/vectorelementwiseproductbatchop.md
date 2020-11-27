# VectorElementwiseProduct 组件

## 功能介绍
 Vector 中的每一个非零元素与scalingVector的每一个对应元素乘，返回乘积后的新vector。

## 算法参数

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| scalingVector | 尺度变化向量。 | 尺度的变化向量。 | String | ✓ |  |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


## 脚本示例

### 运行脚本
```
data = [
    ["1:3,2:4,4:7", 1],
    ["0:3,5:5", 3],
    ["2:4,4:5", 4]]

# load data
data = np.array([["1:3,2:4,4:7", 1],\
    ["0:3,5:5", 3],\
    ["2:4,4:5", 4]])
df = pd.DataFrame({"vec" : data[:,0], "id" : data[:,1]})
data = dataframeToOperator(df, schemaStr="vec string, id bigint",op_type="batch")
vecEP = VectorElementwiseProductBatchOp().setSelectedCol("vec") \
	.setOutputCol("vec1") \
	.setScalingVector("$8$1:3.0 3:3.0 5:4.6")
data.link(vecEP).collectToDataframe()
```
### 运行结果
| vec         | id   | vec1              |
| ----------- | ---- | ----------------- |
| 1:3,2:4,4:7 | 1    | 1:9.0 2:0.0 4:0.0 |
| 0:3,5:5     | 3    | 0:0.0 5:23.0      |
| 2:4,4:5     | 4    | 2:0.0 4:0.0       |
