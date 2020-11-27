# Vector Interaction 组件

## 功能介绍
对两个vector 中的元素两两相乘，并组成一个新的向量。

## 算法参数

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


#### 备注：选择列的数目必须为两列

## 脚本示例

### 运行脚本
``` python
data = np.array([["$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7"],\
    ["$8$0:3,5:5", "$8$1:2,2:4,4:7"],\
    ["$8$2:4,4:5", "$8$1:3,2:3,4:7"]])
df = pd.DataFrame({"vec" : data[:,0], "id" : data[:,1]})
data = dataframeToOperator(df, schemaStr="vec1 string, vec2 string",op_type="stream")
vecInter = VectorInteractionStreamOp().setSelectedCols(["vec1","vec2"]).setOutputCol("vec_product")
data.link(vecInter).print()
StreamOperator.execute()
```
### 运行结果


| vec1           | vec2           | vec_product                              |
| -------------- | -------------- | ---------------------------------------- |
| $8$1:3,2:4,4:7 | $8$1:3,2:4,4:7 | $64$9:9.0 10:12.0 12:21.0 17:12.0 18:16.0 20:28.0 33:21.0 34:28.0 36:49.0 |
| $8$0:3,5:5     | $8$1:2,2:4,4:7 | $64$8:6.0 13:10.0 16:12.0 21:20.0 32:21.0 37:35.0 |
| $8$2:4,4:5     | $8$1:3,2:3,4:7 | $64$10:12.0 12:15.0 18:12.0 20:15.0 34:28.0 36:35.0 |
