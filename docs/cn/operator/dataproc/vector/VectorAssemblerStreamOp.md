# VectorAssembler

## 功能介绍
* 数据结构转换组件，将Table格式的数据转成tensor格式数据。
* 支持table中的多个 vetor 列和数值列合并成一个vector 列。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| handleInvalidMethod | 处理无效值的方法 | 处理无效值的方法，可取 error, skip | String |  | "ERROR" |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| outputCol | 输出结果列列名 | 输出结果列列名，必选 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |


## 脚本示例
### 运行脚本
``` python
data = np.array([\
[2, 1, 1],\
[3, 2, 1],\
[4, 3, 2],\
[2, 4, 1],\
[2, 2, 1],\
[4, 3, 2],\
[1, 2, 1],\
[5, 3, 3]])
df = pd.DataFrame({"f0":data[:,0], "f1":data[:,1], "f2":data[:,2]})
data = dataframeToOperator(df, schemaStr="f0 int, f1 int, f2 int",op_type="stream")
colnames = ["f0","f1","f2"]
VectorAssemblerStreamOp().setSelectedCols(colnames)\
.setOutputCol("out").linkFrom(data).print()
StreamOperator.execute()
```

### 运行结果
f0 | f1 | f2 | out
---|----|----|----
2|1|1|2.0,1.0,1.0
3|2|1|3.0,2.0,1.0
4|3|2|4.0,3.0,2.0
2|4|1|2.0,4.0,1.0
2|2|1|2.0,2.0,1.0
4|3|2|4.0,3.0,2.0
1|2|1|1.0,2.0,1.0
5|3|3|5.0,3.0,3.0






