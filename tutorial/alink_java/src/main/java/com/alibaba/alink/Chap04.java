package com.alibaba.alink;

import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ObjectPath;

import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.catalog.DerbyCatalog;
import com.alibaba.alink.common.io.catalog.HiveCatalog;
import com.alibaba.alink.common.io.catalog.MySqlCatalog;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.CatalogSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CatalogSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CatalogSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CatalogSourceStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.params.io.HasCatalogObject.CatalogObject;

import java.io.File;
import java.util.HashMap;
import java.util.List;

public class Chap04 {

	static final String DATA_DIR = Utils.ROOT_DIR + "db" + File.separator;

	static final String ALINK_PLUGIN_DIR = "/Users/yangxu/Downloads/alink_plugin";

	static final String IRIS_URL =
		"http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data";
	static final String IRIS_SCHEMA_STR =
		"sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

	static final String DB_NAME = "test_db";
	static final String BATCH_TABLE_NAME = "batch_table";
	static final String STREAM_TABLE_NAME = "stream_table";

	static final String HIVE_VERSION = "2.3.4";
	static final String HIVE_CONF_DIR = null;

	static final String DERBY_VERSION = "10.6.1.0";
	static final String DERBY_DIR = "derby";

	static final String MYSQL_VERSION = "5.1.27";
	static final String MYSQL_URL = null;
	static final String MYSQL_PORT = null;
	static final String MYSQL_USER_NAME = null;
	static final String MYSQL_PASSWORD = null;

	static PluginDownloader DOWNLOADER;

	public static void main(String[] args) throws Exception {

		if (null != ALINK_PLUGIN_DIR) {

			AlinkGlobalConfiguration.setPluginDir(ALINK_PLUGIN_DIR);

			AlinkGlobalConfiguration.setPrintProcessInfo(true);
			DOWNLOADER = AlinkGlobalConfiguration.getPluginDownloader();
			DOWNLOADER.downloadPlugin("hive", HIVE_VERSION);
			DOWNLOADER.downloadPlugin("derby", DERBY_VERSION);
			DOWNLOADER.downloadPlugin("mysql", MYSQL_VERSION);

			c_2();

			c_3();

			c_4();
		}

	}

	static void c_2() throws Exception {

		if (null != HIVE_CONF_DIR) {
			HiveCatalog hive = new HiveCatalog("hive_catalog", null, HIVE_VERSION, HIVE_CONF_DIR);

			hive.open();

			hive.createDatabase(DB_NAME, new CatalogDatabaseImpl(new HashMap <>(), ""), true);

			hive.dropTable(new ObjectPath(DB_NAME, BATCH_TABLE_NAME), true);
			hive.dropTable(new ObjectPath(DB_NAME, STREAM_TABLE_NAME), true);

			new CsvSourceBatchOp()
				.setFilePath(IRIS_URL)
				.setSchemaStr(IRIS_SCHEMA_STR)
				.lazyPrintStatistics("< origin data >")
				.link(
					new CatalogSinkBatchOp()
						.setCatalogObject(new CatalogObject(hive, new ObjectPath(DB_NAME, BATCH_TABLE_NAME)))
				);
			BatchOperator.execute();

			new CsvSourceStreamOp()
				.setFilePath(IRIS_URL)
				.setSchemaStr(IRIS_SCHEMA_STR)
				.link(
					new CatalogSinkStreamOp()
						.setCatalogObject(new CatalogObject(hive, new ObjectPath(DB_NAME, STREAM_TABLE_NAME)))
				);
			StreamOperator.execute();

			new CatalogSourceBatchOp()
				.setCatalogObject(new CatalogObject(hive, new ObjectPath(DB_NAME, BATCH_TABLE_NAME)))
				.lazyPrintStatistics("< batch catalog source >");
			BatchOperator.execute();

			new CatalogSourceStreamOp()
				.setCatalogObject(new CatalogObject(hive, new ObjectPath(DB_NAME, STREAM_TABLE_NAME)))
				.sample(0.02)
				.print();
			StreamOperator.execute();

			System.out.println("< tables before drop >");
			System.out.println(JsonConverter.toJson(hive.listTables(DB_NAME)));

			if (hive.tableExists(new ObjectPath(DB_NAME, BATCH_TABLE_NAME))) {
				hive.dropTable(new ObjectPath(DB_NAME, BATCH_TABLE_NAME), false);
			}
			hive.dropTable(new ObjectPath(DB_NAME, STREAM_TABLE_NAME), true);

			System.out.println("< tables after drop >");
			System.out.println(JsonConverter.toJson(hive.listTables(DB_NAME)));

			hive.dropDatabase(DB_NAME, true);
			hive.close();
		}
	}

	static void c_3() throws Exception {

		DerbyCatalog derby = new DerbyCatalog("derby_catalog", null, DERBY_VERSION, DATA_DIR + DERBY_DIR);

		derby.open();

		derby.createDatabase(DB_NAME, new CatalogDatabaseImpl(new HashMap<>(), ""), true);
		derby.dropTable(new ObjectPath(DB_NAME, BATCH_TABLE_NAME), true);
		derby.dropTable(new ObjectPath(DB_NAME, STREAM_TABLE_NAME), true);
		new CsvSourceBatchOp()
			.setFilePath(IRIS_URL)
			.setSchemaStr(IRIS_SCHEMA_STR)
			.lazyPrintStatistics("< origin data >")
			.link(
				new CatalogSinkBatchOp()
					.setCatalogObject(new CatalogObject(derby, new ObjectPath(DB_NAME, BATCH_TABLE_NAME)))
			);
		BatchOperator.execute();

		new CsvSourceStreamOp()
			.setFilePath(IRIS_URL)
			.setSchemaStr(IRIS_SCHEMA_STR)
			.link(
				new CatalogSinkStreamOp()
					.setCatalogObject(new CatalogObject(derby, new ObjectPath(DB_NAME, STREAM_TABLE_NAME)))
			);
		StreamOperator.execute();

		new CatalogSourceBatchOp()
			.setCatalogObject(new CatalogObject(derby, new ObjectPath(DB_NAME, BATCH_TABLE_NAME)))
			.lazyPrintStatistics("< batch catalog source >");
		BatchOperator.execute();

		new CatalogSourceStreamOp()
			.setCatalogObject(new CatalogObject(derby, new ObjectPath(DB_NAME, STREAM_TABLE_NAME)))
			.sample(0.02)
			.print();
		StreamOperator.execute();

		System.out.println("< tables before drop >");
		System.out.println(JsonConverter.toJson(derby.listTables(DB_NAME)));

		if (derby.tableExists(new ObjectPath(DB_NAME, BATCH_TABLE_NAME))) {
			derby.dropTable(new ObjectPath(DB_NAME, BATCH_TABLE_NAME), false);
		}
		derby.dropTable(new ObjectPath(DB_NAME, STREAM_TABLE_NAME), true);

		System.out.println("< tables after drop >");
		System.out.println(JsonConverter.toJson(derby.listTables(DB_NAME)));

		derby.dropDatabase(DB_NAME, true);
		derby.close();
	}

	static void c_4() throws Exception {

		if (null != MYSQL_URL) {
			MySqlCatalog mySql = new MySqlCatalog("mysql_catalog", null, MYSQL_VERSION,
				MYSQL_URL, MYSQL_PORT, MYSQL_USER_NAME, MYSQL_PASSWORD);

			mySql.open();

			mySql.createDatabase(DB_NAME, new CatalogDatabaseImpl(new HashMap <>(), ""), true);

			new CsvSourceBatchOp()
				.setFilePath(IRIS_URL)
				.setSchemaStr(IRIS_SCHEMA_STR)
				.lazyPrintStatistics("< origin data >")
				.link(
					new CatalogSinkBatchOp()
						.setCatalogObject(new CatalogObject(mySql, new ObjectPath(DB_NAME, BATCH_TABLE_NAME)))
				);
			BatchOperator.execute();

			new CsvSourceStreamOp()
				.setFilePath(IRIS_URL)
				.setSchemaStr(IRIS_SCHEMA_STR)
				.link(
					new CatalogSinkStreamOp()
						.setCatalogObject(new CatalogObject(mySql, new ObjectPath(DB_NAME, STREAM_TABLE_NAME)))
				);
			StreamOperator.execute();

			new CatalogSourceBatchOp()
				.setCatalogObject(new CatalogObject(mySql, new ObjectPath(DB_NAME, BATCH_TABLE_NAME)))
				.lazyPrintStatistics("< batch catalog source >");
			BatchOperator.execute();

			new CatalogSourceStreamOp()
				.setCatalogObject(new CatalogObject(mySql, new ObjectPath(DB_NAME, STREAM_TABLE_NAME)))
				.sample(0.02)
				.print();
			StreamOperator.execute();

			System.out.println("< tables before drop >");
			System.out.println(JsonConverter.toJson(mySql.listTables(DB_NAME)));

			if (mySql.tableExists(new ObjectPath(DB_NAME, BATCH_TABLE_NAME))) {
				mySql.dropTable(new ObjectPath(DB_NAME, BATCH_TABLE_NAME), false);
			}
			mySql.dropTable(new ObjectPath(DB_NAME, STREAM_TABLE_NAME), true);

			System.out.println("< tables after drop >");
			System.out.println(JsonConverter.toJson(mySql.listTables(DB_NAME)));

			mySql.dropDatabase(DB_NAME, true);
			mySql.close();
		}
	}

}
