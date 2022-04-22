# Catalog数据库数据源 (CatalogSourceStreamOp)
Java 类名：com.alibaba.alink.operator.stream.source.CatalogSourceStreamOp

Python 类名：CatalogSourceStreamOp


## 功能介绍
Catalog描述了数据库的属性和数据库的位置, 支持Mysql, Derby, Sqlite, Hive.

在使用时，需要先下载插件，详情请看https://www.yuque.com/pinshu/alink_guide/czg4cx

定义分成三步：

第一步，定义Catalog

| 数据库 | Java 接口 |
|---|---|
|Derby | DerbyCatalog(String catalogName, String defaultDatabase, String derbyVersion, String derbyPath)
|MySql | MySqlCatalog(String catalogName, String defaultDatabase, String mysqlVersion,String mysqlUrl, String port, String userName, String password)
|Sqlite | SqliteCatalog(String catalogName, String defaultDatabase, String sqliteVersion, String dbUrl)
|Hive | HiveCatalog(String catalogName, String defaultDatabase, String hiveVersion, String hiveConfDir) <br> <br> HiveCatalog(String catalogName, String defaultDatabase, String hiveVersion, FilePath hiveConfDir) <br> <br> HiveCatalog(String catalogName, String defaultDatabase, String hiveVersion, String hiveConfDir,String kerberosPrincipal, String kerberosKeytab)


    示例：
        derby = DerbyCatalog("derby_test_catalog", DERBY_SCHEMA, "10.6.1.0", derbyFolder+'/'+DERBY_DB)

    各插件提供的版本：
        Hive：2.3.4
        MySQL: 5.1.27
        Derby: 10.6.1.0
        SQLite: 3.19.3
        odps: 0.36.4-public
  

第二步， 定义CatalogObject


```Python
dbName = "sqlite_db"
tableName = "table"

# 第一个参数是Catalog, 第二个参数是DB/Project
catalogObject = CatalogObject(derby, ObjectPath(dbName, tableName))
```

第三步，定义Source和Sink



## 参数说明

| 名称 | 中文名称 | 描述 | 类型 | 是否必须？ | 取值范围 | 默认值 |
| --- | --- | --- | --- | --- | --- | --- |
| catalogObject | catalog object | catalog object | String | ✓ |  |  |


## 代码示例

** 以下代码仅用于示意，可能需要修改部分代码或者配置环境后才能正常运行！**

### Python 代码
#### Derby
```python
derbyFolder = "*"
DERBY_SCHEMA = "derby_schema"
DERBY_DB = "derby_db"
derby = DerbyCatalog("derby_test_catalog", DERBY_SCHEMA, "10.6.1.0", derbyFolder+'/'+DERBY_DB)

catalogObject = CatalogObject(derby, ObjectPath("test_catalog_source_sink", "test_catalog_source_sink3"))

# source sink op
catalogSinkStreamOp = CatalogSinkStreamOp()\
    .setCatalogObject(catalogObject)
     

source.link(catalogSinkStreamOp)

StreamOperator.execute()

# source stream op
catalogSourceStreamOp = CatalogSourceStreamOp()\
    .setCatalogObject(catalogObject)

catalogSourceStreamOp.print()

StreamOperator.execute()

```

#### Java 代码

```java
String derbyFolder = "*";
String DERBY_SCHEMA = "derby_schema";
String DERBY_DB = "derby_db";
DerbyCatalog derby = new DerbyCatalog("derby_test_catalog", DERBY_SCHEMA, "10.6.1.0",
	derbyFolder + '/' + DERBY_DB);

CatalogObject catalogObject = new CatalogObject(derby,
	new ObjectPath("test_catalog_source_sink", "test_catalog_source_sink3"));

CatalogSinkStreamOp catalogSinkStreamOp = new CatalogSinkStreamOp()
	.setCatalogObject(catalogObject);

source.link(catalogSinkStreamOp);

StreamOperator.execute();

CatalogSourceStreamOp catalogSourceStreamOp = new CatalogSourceStreamOp()
	.setCatalogObject(catalogObject);

catalogSourceStreamOp.print();

StreamOperator.execute();
```

### Sqlite

####  Python 代码

```python
sqliteFolder = "*"
SQLITE_SCHEMA = "sqlite_schema"
SQLITE_DB = "sqlite_db"
sqlite = SqliteCatalog("sqlite_test_catalog", None, "3.19.3",  [sqliteFolder+'/'+SQLITE_DB])

catalogObject = CatalogObject(sqlite, ObjectPath(SQLITE_DB, "test_catalog_source_sink3"))

# source sink op
catalogSinkStreamOp = CatalogSinkStreamOp()\
    .setCatalogObject(catalogObject)
     

source.link(catalogSinkBatchOp)

StreamOperator.execute()

# source stream op
catalogSourceStreamOp = CatalogSourceStreamOp()\
    .setCatalogObject(catalogObject)

catalogSourceStreamOp.print()

StreamOperator.execute()

```

#### Java代码

```java
String sqliteFolder = "*";
String SQLITE_SCHEMA = "sqlite_schema";
String SQLITE_DB = "sqlite_db";
SqliteCatalog sqlite = new SqliteCatalog("sqlite_test_catalog", null, "3.19.3", sqliteFolder + '/' +
	SQLITE_DB);

CatalogObject catalogObject = new CatalogObject(sqlite, new ObjectPath(SQLITE_DB,
	"test_catalog_source_sink3"));

CatalogSinkStreamOp catalogSinkStreamOp = CatalogSinkStreamOp()
	.setCatalogObject(catalogObject);

source.link(catalogSinkBatchOp);

StreamOperator.execute();

CatalogSourceStreamOp catalogSourceStreamOp = new CatalogSourceStreamOp()
	.setCatalogObject(catalogObject);

catalogSourceStreamOp.print();

StreamOperator.execute();
```
