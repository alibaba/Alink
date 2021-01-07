# 读CSV文件

## 功能介绍
读CSV文件。支持从本地、hdfs、http读取

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 默认值 |
| --- | --- | --- | --- | --- | --- |
| filePath | 文件路径 | 文件路径 | String | ✓ |  |
| schemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |
| fieldDelimiter | 字段分隔符 | 字段分隔符 | String |  | "," |
| quoteChar | 引号字符 | 引号字符 | Character |  | "\"" |
| skipBlankLine | 是否忽略空行 | 是否忽略空行 | Boolean |  | true |
| rowDelimiter | 行分隔符 | 行分隔符 | String |  | "\n" |
| ignoreFirstLine | 是否忽略第一行数据 | 是否忽略第一行数据 | Boolean |  | false |
| lenient | 是否容错 | 若为true，当解析失败时丢弃该数据；若为false，解析失败是抛异常 | Boolean |  | false |

#### 支持的字段类型包括：

| 字段类型 | 描述 | 值域 | Flink类型 | Java类型 |
| --- | --- | --- | --- | --- |
| VARCHAR/STRING | 可变长度字符串 | 最大容量为4mb | Types.STRING | java.lang.String |
| BOOLEAN | 逻辑值 | 值：TRUE，FALSE，UNKNOWN | Types.BOOLEAN | java.lang.Boolean |
| TINYINT | 微整型，1字节整数 | 范围是-128到127 | Types.BYTE | java.lang.Byte |
| SMALLINT | 短整型，2字节整数 | 范围为-32768至32767 | Types.SHORT | java.lang.Short |
| INT | 整型，4字节整数 | 范围是-2147483648到2147483647 | Types.INT | java.lang.Integer |
| BIGINT/LONG | 长整型，8字节整数 | 范围是-9223372036854775808至9223372036854775807 | Types.LONG | java.lang.Long |
| FLOAT | 4字节浮点数 | 6位数字精度 | Types.FLOAT | java.lang.Float |
| DOUBLE | 8字节浮点数 | 15位十进制精度 | Types.DOUBLE | java.lang.Double |
| DECIMAL | 小数类型 | 示例：123.45是DECIMAL(5,2)值 | Types.DECIMAL | java.math.BigDecimal |
| DATE | 日期 | 示例：'1969-07-20' | Types.SQL_DATE | java.sql.Date |
| TIME | 时间 | 示例：'20:17:40' | Types.SQL_TIME | java.sql.Time |
| TIMESTAMP | 时间戳，日期和时间 | 示例：'1969-07-20 20:17:40' | Types.SQL_TIMESTAMP | java.sql.Timestamp |

#### 关于分隔符的说明：

Web前端支持用户输入如下转义字符和unicode字符作为分隔符：

| 输入分隔符 | 含义 |
| --- | --- |
| \\t | 制表符(tab键) |
| \\n | 换行符 |
| \\b | 退格符 |
| \\r | 回车 |
| \\f | 换页 |
| \\\\ | 反斜线字符 |
| ' | 单引号 |
| " | 双引号 |
| \\ddd | 1到3位八进制数所代表的任意字符，例如\\001表示'ctrl + A', \\40表示空格符 |
| \\udddd | 1到4位十六进制数所代表unicode字符，例如\\u0001表示'ctrl + A', \\u0020表示空格符 |


## 脚本示例

#### Csv Batch Source

```python
filePath = 'http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv'
schema = 'sepal_length double, sepal_width double, petal_length double, petal_width double, category string'
csvSource = CsvSourceBatchOp()\
    .setFilePath(filePath)\
    .setSchemaStr(schema)\
    .setFieldDelimiter(",")
BatchOperator.collectToDataframe(csvSource)
```

#### 脚本运行结果

```python
sepal_length	sepal_width	petal_length	petal_width	category
0	6.3	3.3	6.0	2.5	Iris-virginica
1	5.6	2.8	4.9	2.0	Iris-virginica
2	5.0	3.3	1.4	0.2	Iris-setosa
3	5.8	2.7	5.1	1.9	Iris-virginica
4	7.0	3.2	4.7	1.4	Iris-setosa
```

#### Csv Stream Source

```python
filePath = 'http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv'
schema = 'sepal_length double, sepal_width double, petal_length double, petal_width double, category string'
csvSource = CsvSourceStreamOp(
    Params()
        .set("filePath", filePath)
        .set("schema", schema)
        .set("fieldDelimiter", ",")
)
csvSource.print()
StreamOperator.execute()
```

#### 脚本运行结果

```python
sepal_length	sepal_width	petal_length	petal_width	category
1	5.5	2.4	3.8	1.1	Iris-versicolor
2	6.1	2.6	5.6	1.4	Iris-virginica
3	6.0	2.2	4.0	1.0	Iris-versicolor
4	5.5	2.4	3.7	1.0	Iris-versicolor
5	4.6	3.1	1.5	0.2	Iris-setosa
```



## 公共数据源列表

- iris数据集

filePath： [http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv)

schema： sepal_length double, sepal_width double, petal_length double, petal_width double, category string

- adult数据集

训练数据filePath：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/adult_train.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/adult_train.csv)

训练数据schema：age bigint, workclass string, fnlwgt bigint, education string, education_num bigint, marital_status string, occupation string, relationship string, race string, sex string, capital_gain bigint, capital_loss bigint, hours_per_week bigint, native_country string, label string

预测数据filePath：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/adult_test.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/adult_test.csv)

预测数据schema：age bigint, workclass string, fnlwgt bigint, education string, education_num bigint, marital_status string, occupation string, relationship string, race string, sex string, capital_gain bigint, capital_loss bigint, hours_per_week bigint, native_country string, label string

- mnist数据集

filePath：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/mnist.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/mnist.csv)

schema：label string, content string

- movielens数据集

评分数据filePath：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/movielens_ratings.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/movielens_ratings.csv)

评分数据schema：userid string, movieid string, rating double, timestamp string

电影数据filePath：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/movielens_movies.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/movielens_movies.csv)

电影数据schema：movieid string, title string, genres string

电影数据fieldDelimiter：\t

- 亚马逊product review数据集

filePath：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/review_rating_train.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/review_rating_train.csv)

schema：review_id int, rating5 int, rating3 int, review_context string

fieldDelimiter：_alink_

- 域名检测数据集

数据集1：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/tdl_alexa_domain.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/tdl_alexa_domain.csv)

数据集2：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/tdl_sample_dga.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/tdl_sample_dga.csv)

数据集3：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/tdl_train_domain_data.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/tdl_train_domain_data.csv)

数据集4：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/tdl_domain_test_data.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/tdl_domain_test_data.csv)

- 支付宝公开交易数据集（来自Tianchi Data Lab: tianchi.aliyun.com/datalab）

filePath：[http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/tradeRecords_1kw.csv](http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/tradeRecords_1kw.csv)

schema：user_id &nbsp;string, province &nbsp;string, datetime &nbsp;string, average_pay bigint, score double, comment_cnt bigint, shop_level_int bigint, catalog &nbsp;string, cate_2_name string, cate_3_name string, shop_level string
