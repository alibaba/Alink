## 功能介绍

- 本算子对数据进行随机抽样，每个样本都以相同的概率被抽到。



## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| ratio | 采样比例 | 采样率，范围为[0, 1] | Double | ✓ |  |
| withReplacement | 是否放回 | 是否有放回的采样，默认不放回 | Boolean |  | false |



## 脚本示例

### 脚本代码

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

sampleOp = SampleBatchOp()\
        .setRatio(0.3)\
        .setWithReplacement(False)

inOp.link(sampleOp).print()
```
### 脚本运行结果

|Y|
|---|
|0,0,0|
|0.2,0.2,0.2|




