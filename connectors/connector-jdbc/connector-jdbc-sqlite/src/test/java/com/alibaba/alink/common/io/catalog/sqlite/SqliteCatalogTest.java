package com.alibaba.alink.common.io.catalog.sqlite;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.params.shared.HasOverwriteSink;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;

public class SqliteCatalogTest {

	@ClassRule
	public static final TemporaryFolder SQLITE_FOLDER = new TemporaryFolder();

	/**
	 * Ref: https://www.sqlite.org/lang_createtable.html
	 * <p>
	 * Every CREATE TABLE statement must specify a name for the new table. Table names that begin with "sqlite_" are
	 * reserved for internal use. It is an error to attempt to create a table with a name that starts with "sqlite_".
	 */
	private static final String SQLITE_DB = "test_sqlite_db";
	private static final String SQLITE_DB_1 = "test_sqlite_db_1";
	private static final String SQLITE_DB_TABLE = "test_sqlite_db_table";
	private static final TableSchema SQLITE_DB_TABLE_SCHEMA
		= new TableSchema(new String[] {"col0"}, new TypeInformation <?>[] {Types.STRING});
	private static final String SQLITE_DB_TABLE_1 = "test_sqlite_db_table_1";

	private static SqliteCatalog sqlite;

	@BeforeClass
	public static void beforeClassSqlite() {
		sqlite = new SqliteCatalog(
			"test_sqlite_catalog", null,
			new String[] {new Path(SQLITE_FOLDER.getRoot().getAbsolutePath(), SQLITE_DB).getPath()},
			null, null
		);

		sqlite.open();
	}

	@AfterClass
	public static void afterClassSqlite() {
		sqlite.close();
	}

	@Test
	public void listDatabases() {
		Assert.assertTrue(sqlite.listDatabases().contains(SQLITE_DB));
	}

	@Test
	public void getDatabase() throws DatabaseNotExistException {
		Assert.assertNull(sqlite.getDatabase(SQLITE_DB).getComment());
	}

	@Test
	public void databaseExists() {
		Assert.assertTrue(sqlite.databaseExists(SQLITE_DB));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void createDatabase() throws DatabaseAlreadyExistException {
		sqlite.createDatabase(SQLITE_DB_1, new CatalogDatabaseImpl(Collections.emptyMap(), ""), true);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void dropDatabase() throws DatabaseNotEmptyException, DatabaseNotExistException {
		sqlite.dropDatabase(SQLITE_DB_1, true);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterDatabase() throws DatabaseNotExistException {
		sqlite.alterDatabase(SQLITE_DB_1, new CatalogDatabaseImpl(Collections.emptyMap(), ""), true);
	}

	@Test
	public void listTables() throws TableAlreadyExistException, DatabaseNotExistException {
		sqlite.createTable(
			new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE),
			new CatalogTableImpl(SQLITE_DB_TABLE_SCHEMA, Collections.emptyMap(), ""),
			true
		);

		Assert.assertTrue(sqlite.listTables(SQLITE_DB).contains(SQLITE_DB_TABLE));
	}

	@Test
	public void listViews() throws DatabaseNotExistException {
		Assert.assertTrue(sqlite.listViews(SQLITE_DB).isEmpty());
	}

	@Test
	public void getTable() throws TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
		sqlite.createTable(
			new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE),
			new CatalogTableImpl(SQLITE_DB_TABLE_SCHEMA, Collections.emptyMap(), ""),
			true
		);

		Assert.assertEquals(
			SQLITE_DB_TABLE_SCHEMA,
			sqlite.getTable(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE)).getSchema()
		);
	}

	@Test
	public void tableExists() throws TableAlreadyExistException, DatabaseNotExistException {
		sqlite.createTable(
			new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE),
			new CatalogTableImpl(SQLITE_DB_TABLE_SCHEMA, Collections.emptyMap(), ""),
			true
		);

		Assert.assertTrue(sqlite.tableExists(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE)));
	}

	@Test
	public void dropTable() throws TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
		sqlite.createTable(
			new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE),
			new CatalogTableImpl(SQLITE_DB_TABLE_SCHEMA, Collections.emptyMap(), ""),
			true
		);

		Assert.assertTrue(sqlite.tableExists(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE)));

		sqlite.dropTable(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE), true);

		Assert.assertFalse(sqlite.tableExists(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE)));
	}

	@Test
	public void renameTable() throws TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
		sqlite.createTable(
			new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE),
			new CatalogTableImpl(SQLITE_DB_TABLE_SCHEMA, Collections.emptyMap(), ""),
			true
		);

		sqlite.dropTable(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE_1), true);

		sqlite.renameTable(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE), SQLITE_DB_TABLE_1, true);

		Assert.assertTrue(sqlite.tableExists(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE_1)));
	}

	@Test
	public void createTable() throws TableAlreadyExistException, DatabaseNotExistException {
		sqlite.createTable(
			new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE),
			new CatalogTableImpl(SQLITE_DB_TABLE_SCHEMA, Collections.emptyMap(), ""),
			true
		);

		Assert.assertTrue(sqlite.tableExists(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE)));

	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterTable() throws TableNotExistException {
		sqlite.alterTable(null, null, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void listPartitions() throws TableNotPartitionedException, TableNotExistException {
		sqlite.listPartitions(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testListPartitions() throws TableNotPartitionedException, TableNotExistException {
		sqlite.listPartitions(null, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getPartition() throws PartitionNotExistException {
		sqlite.getPartition(null, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void partitionExists() {
		sqlite.partitionExists(null, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void createPartition()
		throws TableNotPartitionedException, PartitionAlreadyExistsException, TableNotExistException,
		PartitionSpecInvalidException {
		sqlite.createPartition(null, null, null, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void dropPartition() throws PartitionNotExistException {

		sqlite.dropPartition(null, null, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterPartition() throws PartitionNotExistException {
		sqlite.alterPartition(null, null, null, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void listFunctions() throws DatabaseNotExistException {
		sqlite.listFunctions(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getFunction() throws FunctionNotExistException {
		sqlite.getFunction(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void functionExists() {
		sqlite.functionExists(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void createFunction() throws FunctionAlreadyExistException, DatabaseNotExistException {
		sqlite.createFunction(null, null, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterFunction() throws FunctionNotExistException {
		sqlite.alterFunction(null, null, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void dropFunction() throws FunctionNotExistException {
		sqlite.dropFunction(null, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getTableStatistics() throws TableNotExistException {
		sqlite.getTableStatistics(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getTableColumnStatistics() throws TableNotExistException {
		sqlite.getTableColumnStatistics(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getPartitionStatistics() throws PartitionNotExistException {
		sqlite.getPartitionStatistics(null, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getPartitionColumnStatistics() throws PartitionNotExistException {
		sqlite.getPartitionColumnStatistics(null, null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterTableStatistics() throws TableNotExistException {
		sqlite.alterTableStatistics(null, null, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterTableColumnStatistics() throws TablePartitionedException, TableNotExistException {
		sqlite.alterTableColumnStatistics(null, null, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterPartitionStatistics() throws PartitionNotExistException {
		sqlite.alterPartitionStatistics(null, null, null, false);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterPartitionColumnStatistics() throws PartitionNotExistException {
		sqlite.alterPartitionColumnStatistics(null, null, null, false);
	}

	@Test
	public void sinkStream() throws Exception {
		Row[] rows = new Row[] {
			Row.of(
				new byte[] {0, 1},
				new BigDecimal("0.00"),
				(byte) 0,
				(short) 0,
				0,
				0.0f,
				0.0,
				0,
				new Date(0),
				new Time(0),
				new Timestamp(0),
				"string",
				"string",
				new byte[] {0, 1},
				"s",
				new byte[] {0, 1},
				false,
				0L
			)
		};

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(
			Arrays.asList(rows),
			new TableSchema(
				new String[] {
					"col_bit",
					"col_decimal",
					"col_tinyint",
					"col_smallint",
					"col_int",
					"col_float",
					"col_double",
					"col_mediumint",
					"col_date",
					"col_time",
					"col_timestamp",
					"col_text",
					"col_varchar",
					"col_varbinary",
					"col_char",
					"col_binary",
					"col_boolean",
					"col_long"
				},
				new TypeInformation <?>[] {
					Types.PRIMITIVE_ARRAY(Types.BYTE),
					Types.BIG_DEC,
					Types.BYTE,
					Types.SHORT,
					Types.INT,
					Types.FLOAT,
					Types.DOUBLE,
					Types.INT,
					Types.SQL_DATE,
					Types.SQL_TIME,
					Types.SQL_TIMESTAMP,
					Types.STRING,
					Types.STRING,
					Types.PRIMITIVE_ARRAY(Types.BYTE),
					Types.STRING,
					Types.PRIMITIVE_ARRAY(Types.BYTE),
					Types.BOOLEAN,
					Types.LONG
				}
			)
		);

		sqlite.sinkStream(
			new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE_1),
			memSourceStreamOp.getOutputTable(),
			new Params().set(HasOverwriteSink.OVERWRITE_SINK, true),
			memSourceStreamOp.getMLEnvironmentId()
		);

		StreamOperator.execute();

		new TableSourceStreamOp(
			sqlite.sourceStream(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE_1), new Params(),
				MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID)
		).print();

		StreamOperator.execute();
	}

	@Test
	public void sinkBatch() throws Exception {
		Row[] rows = new Row[] {
			Row.of(
				new byte[] {0, 1},
				new BigDecimal("0.00"),
				(byte) 0,
				(short) 0,
				0,
				0.0f,
				0.0,
				0,
				new Date(0),
				new Time(0),
				new Timestamp(0),
				"string",
				"string",
				new byte[] {0, 1},
				"s",
				new byte[] {0, 1},
				false,
				0L
			)
		};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			Arrays.asList(rows),
			new TableSchema(
				new String[] {
					"col_bit",
					"col_decimal",
					"col_tinyint",
					"col_smallint",
					"col_int",
					"col_float",
					"col_double",
					"col_mediumint",
					"col_date",
					"col_time",
					"col_timestamp",
					"col_text",
					"col_varchar",
					"col_varbinary",
					"col_char",
					"col_binary",
					"col_boolean",
					"col_long"
				},
				new TypeInformation <?>[] {
					Types.PRIMITIVE_ARRAY(Types.BYTE),
					Types.BIG_DEC,
					Types.BYTE,
					Types.SHORT,
					Types.INT,
					Types.FLOAT,
					Types.DOUBLE,
					Types.INT,
					Types.SQL_DATE,
					Types.SQL_TIME,
					Types.SQL_TIMESTAMP,
					Types.STRING,
					Types.STRING,
					Types.PRIMITIVE_ARRAY(Types.BYTE),
					Types.STRING,
					Types.PRIMITIVE_ARRAY(Types.BYTE),
					Types.BOOLEAN,
					Types.LONG
				}
			)
		);

		sqlite.sinkBatch(
			new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE_1),
			memSourceBatchOp.getOutputTable(),
			new Params().set(HasOverwriteSink.OVERWRITE_SINK, true),
			memSourceBatchOp.getMLEnvironmentId()
		);

		BatchOperator.execute();

		Assert.assertFalse(
			new TableSourceBatchOp(
				sqlite.sourceBatch(new ObjectPath(SQLITE_DB, SQLITE_DB_TABLE_1), new Params(),
					MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID)
			).collect().isEmpty()
		);
	}
}