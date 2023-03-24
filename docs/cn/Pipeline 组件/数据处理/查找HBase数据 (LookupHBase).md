# 查找HBase数据 (LookupHBase)
Java 类名：com.alibaba.alink.pipeline.dataproc.LookupHBase

Python 类名：LookupHBase


## 功能介绍
LookupHBaseBatchOp ，将HBase中的数据取出。
读HBase Plugin版。plugin版本为1.2.12。
读数据时，指定HBase的zookeeper地址，表名称，列簇名称。指定rowkey列（可以是多列）和要读取的数据列和格式（可以写多列）。
在使用时，需要先下载插件，详情请看https://www.yuque.com/pinshu/alink_guide/czg4cx

## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| familyName | 簇值 | 簇值 | String | ✓ |  |  |
| outputSchemaStr | Schema | Schema。格式为"colname coltype[, colname2, coltype2[, ...]]"，例如"f0 string, f1 bigint, f2 double" | String | ✓ |  |  |
| pluginVersion | 插件版本号 | 插件版本号 | String | ✓ |  |  |
| rowKeyCols | rowkey所在列 | rowkey所在列 | String[] | ✓ |  |  |
| tableName | HBase表名称 | HBase表名称 | String | ✓ |  |  |
| zookeeperQuorum | Zookeeper quorum | Zookeeper quorum 地址 | String | ✓ |  |  |
| reservedCols | 算法保留列名 | 算法保留列 | String[] |  |  | null |
| timeout | HBase RPC 超时时间 | HBase RPC 超时时间，单位毫秒 | Integer |  |  | 1000 |

