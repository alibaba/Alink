## Description
Data source of a CSV (Comma Separated Values) file.
 
 The file can reside in places including:
 <ul>
 <li> local file system
 <li> hdfs
 <li> http
 </ul></p>

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| filePath | File path with file system. | String | ✓ |  |
| schemaStr | Formatted schema | String | ✓ |  |
| fieldDelimiter | Field delimiter | String |  | "," |
| quoteChar | quote char | Character |  | "\"" |
| skipBlankLine | skipBlankLine | Boolean |  | true |
| rowDelimiter | Row delimiter | String |  | "\n" |
| ignoreFirstLine | Whether to ignore first line of csv file. | Boolean |  | false |
| lenient | lenient | Boolean |  | false |

## Script Example

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

#### Results

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

#### Results

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
