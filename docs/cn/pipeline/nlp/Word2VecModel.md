## 功能介绍
由 Word2Vec 组件调用 fit 方法产生，详见 Word2Vec 组件的文档。


## 参数说明
| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| selectedCol | 选中的列名 | 计算列对应的列名 | String | ✓ |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  | null |
| outputCol | 输出结果列 | 输出结果列列名，可选，默认null | String |  | null |
| wordDelimiter | 单词分隔符 | 单词之间的分隔符 | String |  | " " |
| predMethod | 向量组合方法 | 预测文档向量时，需要用到的方法。支持三种方法：平均（avg），最小（min）和最大（max），默认值为平均 | String |  | "AVG" |
| numThreads | 组件多线程线程个数 | 组件多线程线程个数 | Integer |  | 1 |


## 脚本示例
见 Word2Vec 组件的文档。
