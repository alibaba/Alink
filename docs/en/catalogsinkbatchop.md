## Description
catalog sink batch op for Hive, Derby, Mysql, Sqlite.

## Parameters
| Name | Description | Type | Required？ | Default Value |
| --- | --- | --- | --- | --- |
| catalogObject | Object in catalog. | String | ✓ |  |

## Script Example

### Code

#### Derby
```python
from pyalink.alink import *
import pandas as pd

useLocalEnv(1, config=None)

derbyFolder = "*"
DERBY_SCHEMA = "derby_schema"
DERBY_DB = "derby_db"
derby = DerbyCatalog("derby_test_catalog", DERBY_SCHEMA, "10.6.1.0", derbyFolder+'/'+DERBY_DB)

catalogObject = CatalogObject(derby, ObjectPath("test_catalog_source_sink", "test_catalog_source_sink3"))

catalogSinkBatchOp = CatalogSinkBatchOp()\
    .setCatalogObject(catalogObject)
     

source.link(catalogSinkBatchOp)

BatchOperator.execute()

catalogSourceBatchOp = CatalogSourceBatchOp()\
    .setCatalogObject(catalogObject)

catalogSourceBatchOp.print()

```

#### Sqlite

```python
sqliteFolder = "*"
SQLITE_SCHEMA = "sqlite_schema"
SQLITE_DB = "sqlite_db"
sqlite = SqliteCatalog("sqlite_test_catalog", None, "3.19.3",  [sqliteFolder+'/'+SQLITE_DB])

catalogObject = CatalogObject(sqlite, ObjectPath(SQLITE_DB, "test_catalog_source_sink3"))

catalogSinkBatchOp = CatalogSinkBatchOp()\
    .setCatalogObject(catalogObject)
     

source.link(catalogSinkBatchOp)

BatchOperator.execute()

catalogSourceBatchOp = CatalogSourceBatchOp()\
    .setCatalogObject(catalogObject)

catalogSourceBatchOp.print()
```
