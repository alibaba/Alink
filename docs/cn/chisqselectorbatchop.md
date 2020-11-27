# 卡方筛选

## 功能介绍

针对table数据，进行特征筛选

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectorType | 筛选类型 | 筛选类型，包含"NumTopFeatures","percentile", "fpr", "fdr", "fwe"五种。 | String |  | "NumTopFeatures" |
| numTopFeatures | 最大的p-value列个数 | 最大的p-value列个数, 默认值50 | Integer |  | 50 |
| percentile | 筛选的百分比 | 筛选的百分比，默认值0.1 | Double |  | 0.1 |
| fpr | p value的阈值 | p value的阈值，默认值0.05 | Double |  | 0.05 |
| fdr | 发现阈值 | 发现阈值, 默认值0.05 | Double |  | 0.05 |
| fwe | 错误率阈值 | 错误率阈值, 默认值0.05 | Double |  | 0.05 |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |


## 脚本示例

#### 脚本代码
```python

data = np.array([
    ["a", 1, 1,2.0, True],
    ["c", 1, 2, -3.0, True],
    ["a", 2, 2,2.0, False],
    ["c", 0, 0, 0.0, False]
])

df = pd.DataFrame({"f_string": data[:, 0], "f_long": data[:, 1], "f_int": data[:, 2], "f_double": data[:, 3], "f_boolean": data[:, 4]})
source = dataframeToOperator(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean', op_type="batch")

selector = ChiSqSelectorBatchOp()\
            .setSelectedCols(["f_string", "f_long", "f_int", "f_double"])\
            .setLabelCol("f_boolean")\
            .setNumTopFeatures(2)

selector.linkFrom(source)

modelInfo: ChisqSelectorModelInfo = selector.collectModelInfo()
        
print(modelInfo.getColNames())


```

#### 脚本运行结果

```
['f_string', 'f_long']
```




