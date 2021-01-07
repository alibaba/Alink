## 功能介绍

卡法独立性检验是检验两个因素（各有两项或以上的分类）之间是否相互影响的问题，其零假设是两因素之间相互独立。

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| labelCol | 标签列名 | 输入表中的标签列名 | String | ✓ |  |
| selectedCols | 选择的列名 | 计算列对应的列名列表 | String[] | ✓ |  |



## 脚本示例

### 脚本代码

```python
data = np.array([
            ['a1','b1','c1'],
            ['a1','b2','c1'],
            ['a1','b1','c2'],
            ['a2','b1','c1'],
            ['a2','b2','c2'],
            ['a2', 'b1','c1']])

df = pd.DataFrame({"x1": data[:, 0], "x2": data[:, 1], "x3": data[:, 2]})
source = dataframeToOperator(df, schemaStr='x1 string, x2 string, x3 string', op_type='batch')

chisqTest = ChiSquareTestBatchOp()\
            .setSelectedCols(["x1","x2"])\
            .setLabelCol("x3")

source.link(chisqTest).print()
```
### 脚本运行结果

col|chi2_result
-----|-----------
x1|{"comment":"pearson test","df":1.0,"p":1.0,"value":0.0}
x2|{"comment":"pearson test","df":1.0,"p":0.5402913746074196,"value":0.37500000000000006}




