/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * 	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.alink.common.io.catalog.datahub.common.errorcode;

/**
 * CON Error Code Comments.
 * CON - 00 - 00 - 0000
 * First Part:
 * Error Type:
 * 00: argument data error
 * 01: source error
 * 02: sink
 * 03: dim table
 * 04：specific connector error
 * 05: parser error
 * Second Part:
 * 00: keep error code consistent
 * Third Part:
 * detail error kind
 */
public interface ConnectorErrorCode {
	// common module
	//con-00xxxxxx
	@ErrorFactory.ErrCode(codeId = "CON-00000001",
			cause = "Unknown type index  {0}",
			details = "",
			action = "please check the field type definition or contact supporter for help.")
	String dataTypeError(String type);

	@ErrorFactory.ErrCode(codeId = "CON-00000002",
			cause = "get data from zookeeper fails",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String getDataFromZookeeperError();

	@ErrorFactory.ErrCode(codeId = "CON-00000003",
			cause = "Lack necessary arguments: {0}",
			details = "",
			action = "Check your config with the user guide.")
	String lackNecessaryArgumentsError(String info);

	@ErrorFactory.ErrCode(codeId = "CON-00000004",
			cause = "Reason: {0}",
			details = "",
			action = "Check the structure.")
	String byteUitlsConvertArgumentError(String reason);

	@ErrorFactory.ErrCode(codeId = "CON-00000005",
			cause = "Primary key: {0} is null!",
			details = "",
			action = "Check your data structure.")
	String primaryKeyIsNullError(String colName);

	@ErrorFactory.ErrCode(codeId = "CON-00000006",
			cause = "SystemError-Internal: record fields number（{0}）doesn''t match sql " +
					"columns（{1}）",
			details = "",
			action = "Check your data structure and contact admin.")
	String jdbcUtilsColumnsMismatchError(String s0, String s1);

	@ErrorFactory.ErrCode(codeId = "CON-00000007",
			cause = "SystemError-Internal:record''s {0} filed (based from 1) data type " +
					"doesn''t match {1} ",
			details = "",
			action = "Check your data structure and contact admin.")
	String jdbcUtilsDatatypeUncompatiableError(String s0, String s1);

	@ErrorFactory.ErrCode(codeId = "CON-00000008",
			cause = "values of primary key: {0} != primary key num: {1}",
			details = "",
			action = "Check your data structure and contact admin.")
	String primaryKeyMatchError(String s0, String s1);

	@ErrorFactory.ErrCode(codeId = "CON-00000009",
			cause = "{0} don't have sufficient arguments",
			details = "",
			action = "please check your config")
	String unSufficientArguments(String source);

	@ErrorFactory.ErrCode(codeId = "CON-00000010",
			cause = "operator timeout ",
			details = "Retrying cost [%d] ms, exceed timeout threshold [%d].",
			action = "Contact admin to check the specific stack error")
	String retryTimeExceedTimeoutError(long actual, long timeout);

	//con-01xxxxxx source error
	@ErrorFactory.ErrCode(codeId = "CON-01000001",
			cause = "create source fail: {0}",
			details = "",
			action = "Inner Error.Please contact admin.")
	String innerCreateSourceError(String source);

	@ErrorFactory.ErrCode(codeId = "CON-01000002",
			cause = "''{0}'' is not supported as a source table.",
			details = "",
			action = "Check the doc for more information")
	String unsupportedSourceError(String connectorType);

	@ErrorFactory.ErrCode(codeId = "CON-01000003",
			cause = "Fail to get input split from master. ",
			details = "",
			action = "Contact System admin for help.")
	String getInputSplitFromJMError();

	@ErrorFactory.ErrCode(codeId = "CON-01000004",
			cause = "Fail to get partition list",
			details = "",
			action = "Check your source table config or contact system admin for help if needed.")
	String getSourcePartitionsError();

	@ErrorFactory.ErrCode(codeId = "CON-01000005",
			cause = "Source {0} partitions number has changed from {1} to {2}",
			details = "",
			action = "please restart the job ")
	String sourcePartitionChangeError(String source, String expected, String actual);

	@ErrorFactory.ErrCode(codeId = "CON-01000006",
			cause = "Source {0} partitions number has changed from {1} to {2}",
			details = "",
			action = "Wait the failover finish, blink is trying to recovery from source partiton change ")
	String sourcePartitionChangeFailOverRecoryError(String source, String expected, String actual);

	//con-02xxxxxx sink error
	@ErrorFactory.ErrCode(codeId = "CON-02000001",
			cause = "create sink fail: {0}",
			details = "",
			action = "Inner Error.Please contact admin.")
	String innerCreateSinkError(String sink);

	@ErrorFactory.ErrCode(codeId = "CON-02000002",
			cause = "''{0}'' is not supported as a source table.",
			details = "",
			action = "Check the doc for more information")
	String unsupportedSinkError(String connectorType);

	//con-03xxxxxx
	@ErrorFactory.ErrCode(codeId = "CON-03000001",
			cause = "create sink fail: {0}",
			details = "",
			action = "Inner Error.Please contact admin.")
	String innerCreateDimError(String dim);

	@ErrorFactory.ErrCode(codeId = "CON-03000002",
			cause = "DimSource must have primary keys",
			details = "",
			action = "please define the primary keys in dimension table.")
	String dimSourceWithoutPKError();

	@ErrorFactory.ErrCode(codeId = "CON-03000003",
			cause = "ALL cache do not support remove operation.",
			details = "",
			action = "Don''t call this method on one2oneAllCacheRef Object.")
	String allCacheRemoveOperateError();

	@ErrorFactory.ErrCode(codeId = "CON-03000004",
			cause = "Unsupported cache type ''{0}'', currently only support ''LRU'' and ''ALL'' cache.",
			details = "",
			action = "Configure the cache type properly.")
	String unsupportedCacheTypeError(String cacheType);

	@ErrorFactory.ErrCode(codeId = "CON-03000005",
			cause = "Illegal time range blacklist ''{0}'', should use ''->'' to separate start time and end time.",
			details = "",
			action = "Check the timeRange Separator in dim table ddl.")
	String incorrectTimeRangeSepError(String str);

	@ErrorFactory.ErrCode(codeId = "CON-03000006",
			cause = "Illegal time range blacklist, " +
					"start time ''{0}'' is greater than end time ''{1}''",
			details = "",
			action = "Check the time range in dim table ddl.")
	String incorrectTimeRangeError(String t1, String t2);

	@ErrorFactory.ErrCode(codeId = "CON-03000007",
			cause = "Parameter ''{0}'' only accept ''unordered'' or ''ordered''.",
			details = "",
			action = "Check the ddl definition in your sql")
	String asyncConfigError(String key);

	@ErrorFactory.ErrCode(codeId = "CON-03000008",
			cause = "Illegal time format: ''{0}'' or ''{1}'' in dim table ddl",
			details = "",
			action = "Check the time format in dim table ddl.")
	String dateFormatError(String t1, String t2);

	@ErrorFactory.ErrCode(codeId = "CON-03000009",
			cause = "''{0}'' is not supported as a source table.",
			details = "",
			action = "Check the doc for more information")
	String unsupportedDimSourceError(String connectorType);

	@ErrorFactory.ErrCode(codeId = "CON-03000010",
			cause = "''{0}'' do not support async join currently.",
			details = "",
			action = "Check the doc for more information")
	String unsupportedAsyncJoinOperateError(String connectorType);

	@ErrorFactory.ErrCode(codeId = "CON-03000011",
			cause = "{0} only support cache {1} ",
			details = "",
			action = "please change your cache strategy to ALL")
	String unsupportedCacheStrategyError(String type, String cache);

	@ErrorFactory.ErrCode(codeId = "CON-03000012",
			cause = "{0} dim table require primary key(s).",
			details = "",
			action = "check your config in ddl")
	String dimTableNoPrimaryKeyError(String name);

	@ErrorFactory.ErrCode(codeId = "CON-03000013",
			cause = "{0} dim table require primary key or unique key or index column(s).",
			details = "",
			action = "check your config in ddl")
	String dimTableNoIndexKeyError(String name);

	//con-04xxxxxx specific connector error

	@ErrorFactory.ErrCode(codeId = "CON-04000001",
			cause = "Can''t connect to ads.",
			details = "",
			action = "Check your ads config and contact ads admin for help.")
	String adsConnectError();

	@ErrorFactory.ErrCode(codeId = "CON-04000002",
			cause = "Error occurs when closing the connection to ads",
			details = "",
			action = "Check your ads config and contact ads admin for help.")
	String adsCloseConnectError();

	@ErrorFactory.ErrCode(codeId = "CON-04000003",
			cause = "Settings Error:  table {0} doesn''t contain or define primary keys",
			details = "",
			action = "Check your ddl config  or contact dba for help.")
	String noPrimaryKeyDefinitonError(String tableName);

	@ErrorFactory.ErrCode(codeId = "CON-04000004",
			cause = "Settings Error:  get table partition info error. DB: {0} Table: {1}",
			details = "",
			action = "Check your ddl config  or contact dba for help.")
	String adsGetPartitionError(String dbName, String tableName);

	@ErrorFactory.ErrCode(codeId = "CON-04000005",
			cause = "Table {0} ddl with clause error, {1} can not be empty or null and must be in valid range",
			details = "",
			action = "Check your ddl config.")
	String tableDDLConfigError(String tableName, String key);

	@ErrorFactory.ErrCode(codeId = "CON-04000006",
			cause = "Write ads error, retry attempts {0}, db {1}, table {2}, sql {3}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String adsWriteError(String retryNums, String db, String table, String sql);

	@ErrorFactory.ErrCode(codeId = "CON-04000007",
			cause = "Delete ads error, db {0}, table {1}, sql {2}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String adsDeleteError(String db, String table, String sql);

	@ErrorFactory.ErrCode(codeId = "CON-04000008",
			cause = "Batch insert ads error",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String adsBatchWriteError();

	@ErrorFactory.ErrCode(codeId = "CON-04000009",
			cause = "Ads partition filed data type convert error. ColName {0}, index  {1}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String adsPartitionDataFormatConfigError(String colName, String index);

	@ErrorFactory.ErrCode(codeId = "CON-04000010",
			cause = "Currently HBase table can only accept one primary key.",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String hbaseDimMultiPKDefinitionError();

	@ErrorFactory.ErrCode(codeId = "CON-04000011",
			cause = "Cannot create connection to HBase.",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String hbaseConnectError();

	@ErrorFactory.ErrCode(codeId = "CON-04000012",
			cause = "Hbase parameter needed. (dataId, dataGroupId) or (zkHosts, zkParents) must be invalid",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String hbaseConnectParameterError();

	@ErrorFactory.ErrCode(codeId = "CON-04000013",
			cause = "Dynamic table require only 3 columns: rowKey,key,value",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String hbaseDynamicTableError();

	@ErrorFactory.ErrCode(codeId = "CON-04000014",
			cause = "Batch write to hbase error",
			details = "",
			action = "Contact admin for help")
	String hbaseBatchWriteError();

	@ErrorFactory.ErrCode(codeId = "CON-04000015",
			cause = "Batch delete hbase error",
			details = "",
			action = "Contact admin for help")
	String hbaseBatchDeleteError();

	@ErrorFactory.ErrCode(codeId = "CON-04000016",
			cause = "Cannot create connection to HBase {0}.",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String hbaseConnectError(String tableName);

	@ErrorFactory.ErrCode(codeId = "CON-04000017",
			cause = "Source {0} partition has changed.",
			details = "",
			action = "restart the job")
	String sourcePartitionChangeError(String source);

	@ErrorFactory.ErrCode(codeId = "CON-04000018",
			cause = "Error occurs when reading data from  datahub, msg: {0}",
			details = "",
			action = "Check the exception")
	String datahubReadDataError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000019",
			cause = "Datahub don''t support type:{0}，columnName:{1}",
			details = "",
			action = "Check the data type both in datahub and blink.")
	String datahubReadDataTypeError(String type, String colName);

	@ErrorFactory.ErrCode(codeId = "CON-04000020",
			cause = "Get source {0} partition error.",
			details = "",
			action = "check the config in with clause.")
	String sourceGetPartitionError(String source);

	@ErrorFactory.ErrCode(codeId = "CON-04000021",
			cause = "The DDL column type not defined, project:{0}，topic:{1}，column：{2}",
			details = "",
			action = "check the config in with clause.")
	String datahubColumnTypeNoDefiendError(String project, String topic, String column);

	@ErrorFactory.ErrCode(codeId = "CON-04000022",
			cause = "The DDL column type not defined, project:{0}，topic:{1}，column：{2}, columnType: {3}",
			details = "",
			action = "check the config in with clause.")
	String unsupportedDatahubColumnTypeError(String project, String topic, String column, String type);

	@ErrorFactory.ErrCode(codeId = "CON-04000023",
			cause = "Write to datahub error, retry attempts {0}, errorMsg {1}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String writeToDatahubError(String retry, String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000024",
			cause = "writer Igraph error, retCode[{0}] retMsg[{1} ",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String igraphWriteError(String retCode, String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000025",
			cause = "Delete Igraph error, retCode[{0}] retMsg[{1} ",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String igraphDeleteError(String retCode, String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000026",
			cause = "Batch write Igraph error, queries{0} ",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String igraphBatchWriteError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000027",
			cause = "Batch delete Igraph error, queries{0} ",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String igraphBatchDeleteError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000028",
			cause = "Metaq/mq currently not support push mode, pls use pull mode.!",
			details = "",
			action = "Contact lindrom admin for help")
	String mqUnsupportedConsumerModeError();

	@ErrorFactory.ErrCode(codeId = "CON-04000029",
			cause = "Can not connector to metaq",
			details = "",
			action = "Check the config in your ddl.")
	String mqConnectError();

	@ErrorFactory.ErrCode(codeId = "CON-04000030",
			cause = "Can not write to metaq/mq, retry attempts {0}, errMsg {1}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String mqWriteError(String retry, String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000031",
			cause = "Start mq/metaq Consumer Error, msg:{0}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String mqStartConsumerError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000032",
			cause = "Read notify exception, msg {0}",
			details = "",
			action = "Contact notify admin for help")
	String notifyReadError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000032",
			cause = "Notify as a source can define and only define one varchar or varbianry data type",
			details = "",
			action = "Modify your sql ddl")
	String notifyContentDefinitionError();

	@ErrorFactory.ErrCode(codeId = "CON-04000033",
			cause = "Error occurs when init {0} connection pool ",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String initConnectionPoolError(String source);

	@ErrorFactory.ErrCode(codeId = "CON-04000034",
			cause = "Batch write oceanBase error, msg{0} ",
			details = "",
			action = "contact ob dba for help.")
	String oceanBaseBatchWriteError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000035",
			cause = "Batch delete oceanBase error, msg{0} ",
			details = "",
			action = "contact ob dba for help.")
	String oceanBaseBatchDeleteError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000036",
			cause = "Write to ots error, failed record num {0} ",
			details = "",
			action = "contact ots admin for help.")
	String otsWriteError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000037",
			cause = "Ots primary key type error, table {0}  primary key {1}",
			details = "",
			action = "contact ots admin for help.")
	String otsPrimaryKeyError(String table, String pk);

	@ErrorFactory.ErrCode(codeId = "CON-04000038",
			cause = "Ots attribute key type error, table {0}  column key {1}",
			details = "",
			action = "contact ots admin for help.")
	String otsAttributeKeysError(String table, String column);

	@ErrorFactory.ErrCode(codeId = "CON-04000039",
			cause = "Ots primary key can not be null, table {0}  column {1}",
			details = "",
			action = "contact ots admin for help.")
	String otsPkIsNullError(String msg, String pk);

	@ErrorFactory.ErrCode(codeId = "CON-04000040",
			cause = "{0} can not get connection or connect to server successfuly",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String rdsGetConnectionError(String rdsType);

	@ErrorFactory.ErrCode(codeId = "CON-04000041",
			cause = "{0} write to db error, sql {1}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String rdsWriteError(String rdsType, String sql);

	@ErrorFactory.ErrCode(codeId = "CON-04000042",
			cause = "{0} create table error",
			details = "",
			action = "check the prvilage of account")
	String rdsCreateTableError(String rdsType);

	@ErrorFactory.ErrCode(codeId = "CON-04000043",
			cause = "{0} create table error, sql {1}",
			details = "",
			action = "check the prvilage of account")
	String rdsCreateTableError(String rdsType, String sql);

	@ErrorFactory.ErrCode(codeId = "CON-04000044",
			cause = "{0} write to db error, sql {1}, prepareStatement {2}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String rdsBatchWriteError(String rdsType, String sql, String prepare);

	@ErrorFactory.ErrCode(codeId = "CON-04000045",
			cause = "{0} write to db error, sql {1}, prepareStatement {2}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String rdsBatchDeleteError(String rdsType, String sql, String prepare);

	@ErrorFactory.ErrCode(codeId = "CON-04000046",
			cause = "Write to sls error,{0}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String slsWriteError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000047",
			cause = "Read data from sls error,{0}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String slsReadError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000048",
			cause = "Sls Internal Error, msg {0}",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String slsInternalError(String msg);

	@ErrorFactory.ErrCode(codeId = "CON-04000049",
			cause = "TT filter express has questions: [{0}] for example,''or'' is mixed up with ''and''",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String ttFilterFormatError(String filter);

	@ErrorFactory.ErrCode(codeId = "CON-04000050",
			cause = "lindorm batch deleteError",
			details = "",
			action = "contact lindorm admin for help")
	String lindormBatchDeleteError();

	@ErrorFactory.ErrCode(codeId = "CON-04000051",
			cause = "cloudHbase update error",
			details = "cloudHbase update error, {0}",
			action = "please contact customer support for this")
	String cloudHbaseInsertError(String moreInfo);

	@ErrorFactory.ErrCode(codeId = "CON-04000052",
			cause = "cloudHbase dynamic table format error",
			details = "cloudHbase dynamic table have and only have three columns, such as(rowkey, key, value)",
			action = "please check the hbase schema and config parameters.")
	String cloudHbaseDynamicTableFormatError();

	@ErrorFactory.ErrCode(codeId = "CON-04000053",
			cause = "ODPS Partition table must indicate partition ",
			details = "",
			action = "")
	String odpsPartitionTableWithoutPartitionError();

	@ErrorFactory.ErrCode(codeId = "CON-04000054",
			cause = "ODPS {0} Partition {1} table count ODPS table's row count exceeds maxRowCount limit {2}",
			details = "",
			action = "please switch to a smaller table ")
	String odpsTableExceedMaxRowCountError(String table, String partition, String maxRowCount);

	@ErrorFactory.ErrCode(codeId = "CON-04000055",
			cause = "{0} error",
			details = "",
			action = "please check your config")
	String esMappingsError(String mappings);

	@ErrorFactory.ErrCode(codeId = "CON-04000056",
			cause = "{0} index or {1} mapping doesn't exist or unknown error",
			details = "",
			action = "please check your config")
	String esIndexOrMappingError(String indexName, String mappings);

	@ErrorFactory.ErrCode(codeId = "CON-04000057",
			cause = "{0} index not found.",
			details = "",
			action = "please check your config")
	String esIndexNotFoundError(String indexNames);

	@ErrorFactory.ErrCode(codeId = "CON-04000058",
			cause = "{0} Hbase  tableName {1} batch operate error",
			details = "",
			action = "Contact admin for help")
	String hbaseBatchOperateError(String type, String tableName);

	@ErrorFactory.ErrCode(codeId = "CON-04000059",
			cause = "Cassandra keyspace {1} sink connector error",
			details = "",
			action = "Contact admin for help")
	String cassandraCreateTableError(String keyspace);

	@ErrorFactory.ErrCode(codeId = "CON-04000060",
			cause = "init error, {1} can not be empty or null and must be in valid range.",
			details = "",
			action = "Contact admin for help")
	String cassandraConfigError(String configName);

	@ErrorFactory.ErrCode(codeId = "CON-04000061",
			cause = "Database don''t support type:{0}，columnName:{1}",
			details = "",
			action = "Check the data type both in db and blink.")
	String databaseReadDataTypeError(String type, String colName);

	//con-05xxxxxx parser error
	@ErrorFactory.ErrCode(codeId = "CON-05000001",
			cause = "Unsupported ''{0}'' encoding charset.",
			details = "",
			action = "Check the encoding configItem in your ddl.")
	String parserUnsupportedEncodingError(String encoding);

	@ErrorFactory.ErrCode(codeId = "CON-05000002",
			cause = "Data format error, field type: {0} field data: {1}, index: {2} + data: [{3}]",
			details = "",
			action = "Check whether the type and actual data format match in your ddl")
	String parserDataFormatError(String type, String field, String index, String data);

	@ErrorFactory.ErrCode(codeId = "CON-05000003",
			cause = "Field missing error, table column number {0},\n data column number:{1}, \n data filed " +
					"number:{2},\n data: [{3}]",
			details = "",
			action = "Please check your source data structure")
	String parserFieldMissingError(String totalColumnSize, String dataColumnSize, String datalength, String data);

	@ErrorFactory.ErrCode(codeId = "CON-05000004",
			cause = "Field increment error, table column number {0},\n data column number:{1}, \n data filed " +
					"number:{2},\n data: [{3}]",
			details = "",
			action = "Please check your source data structure.")
	String parserFieldIncrementError(String totalColumnSize, String dataColumnSize, String datalength, String data);

	@ErrorFactory.ErrCode(codeId = "CON-05000005",
			cause = "{0} is not a boolean. Only true, false, 1, 0 are valid.",
			details = "Boolean definition error in StringSerializer.java",
			action = "Check your data structure and contact admin.")
	String booleanParseError(String s);

	@ErrorFactory.ErrCode(codeId = "CON-05000006",
			cause = "Incorrect datetime format: %s, pls use ISO-8601 " +
					"complete date plus hours, minutes and seconds format:%s",
			details = "",
			action = "Check the cause, and contact system admin for help if necessary ")
	String incorrectTimeFormatError(String startTime);

}
