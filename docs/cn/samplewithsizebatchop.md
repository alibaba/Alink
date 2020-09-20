
## 功能介绍

- 随机采样是对数据进行按个数随机抽样，每个样本都以相同的概率被抽到。


## 参数说明
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| size | 采样个数 | 采样个数 | Integer | ✓ |  |
| withReplacement | 是否放回 | 是否有放回的采样，默认不放回 | Boolean |  | false |<!-- This is the end of auto-generated parameter info -->

## 脚本示例

#### 脚本

```python
data = data = np.array([
       ["0,0,0"],
       ["0.1,0.1,0.1"],
       ["0.2,0.2,0.2"],
       ["9,9,9"],
       ["9.1,9.1,9.1"],
       ["9.2,9.2,9.2"]
])
    
df = pd.DataFrame({"Y": data[:, 0]})

# batch source 
inOp = dataframeToOperator(df, schemaStr='Y string', op_type='batch')

sampleOp = SampleWithSizeBatchOp()\
        .setSize(2)\
        .setWithReplacement(False)

inOp.link(sampleOp).print()
```
#### 结果

|Y|
|---|
|0,0,0|
|0.2,0.2,0.2|




