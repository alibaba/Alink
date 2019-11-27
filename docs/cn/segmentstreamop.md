## 功能介绍

对指定列对应的文章内容进行分词，分词后的各个词语间以空格作为分隔符。用户自定义分词 从参数列表中输入。

## 参数说明
<!-- This is the start of auto-generated parameter info -->
<!-- DO NOT EDIT THIS PART!!! -->
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| userDefinedDict | 用户自定义字典 | 用户自定义字典 | String[] |  | null |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |<!-- This is the end of auto-generated parameter info -->


## 脚本示例
#### 脚本代码
```python
# -*- coding=UTF-8 -*-

data = np.array([
    [0, u'二手旧书:医学电磁成像'],
    [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
    [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
    [3, u'二手中国糖尿病文献索引'],
    [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']
])

df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
inOp = dataframeToOperator(df, schemaStr='id long, text string', op_type='stream')

segment = SegmentStreamOp().setSelectedCol("text").setOutputCol("segment")

segment.linkFrom(inOp).print()

StreamOperator.execute()
```

#### 脚本运行结果

```
	id	text	segment
0	3	二手中国糖尿病文献索引	二手 中国 糖尿病 文献 索引
1	0	二手旧书:医学电磁成像	二手 旧书 : 医学 电磁 成像
2	4	二手郁达夫文集（ 国内版 ）全十二册馆藏书	二手 郁达夫 文集 （ 国内 版 ） 全 十二册 馆藏 书
3	1	二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969	二手 美国 文学 选读 （ 下册 ） 李宜燮 南开大学 出版社 97873100...
4	2	二手正版图解象棋入门/谢恩思主编/华龄出版社	二手 正版 图解 象棋 入门 / 谢恩 思 主编 / 华龄 出版社
```




