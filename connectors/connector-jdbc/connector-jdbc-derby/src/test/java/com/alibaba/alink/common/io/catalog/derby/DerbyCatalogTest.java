package com.alibaba.alink.common.io.catalog.derby;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
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
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.alibaba.alink.testutil.AlinkTestBase;
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

public class DerbyCatalogTest extends AlinkTestBase {

	@ClassRule
	public static TemporaryFolder derbyFolder = new TemporaryFolder();

	private static final String DERBY_DB = "derby_db";
	private static final String DERBY_SCHEMA = "derby_schema";
	private static final String DERBY_DB_1 = "derby_db_1";
	private static final String DERBY_SCHEMA_1 = "derby_schema_1";
	private static final String DERBY_DB_TABLE = "derby_db_table";
	private static final TableSchema DERBY_DB_TABLE_SCHEMA
		= new TableSchema(new String[] {"col0"}, new TypeInformation <?>[] {Types.STRING});
	private static final String DERBY_DB_TABLE_1 = "derby_db_table_1";
	private static final String DERBY_DB_TABLE_2 = "derby_db_table_2";
	private static final String DERBY_DB_FUNCTION = "derby_db_table";

	private static DerbyCatalog derby;

	@BeforeClass
	public static void beforeClassDerby() throws DatabaseAlreadyExistException {
		System.setProperty(
			"derby.stream.error.field",
			new Path(derbyFolder.getRoot().getAbsolutePath(), "derby.log").getPath()
		);

		derby = new DerbyCatalog(
			"derby_test_catalog", DERBY_SCHEMA,
			new Path(derbyFolder.getRoot().getAbsolutePath(), DERBY_DB).getPath(),
			null, null
		);

		derby.open();

		derby.createDatabase(DERBY_SCHEMA, null, true);
	}

	@AfterClass
	public static void afterClassDerby() {
		if (derby != null) {
			derby.close();
		}
	}

	@Test
	public void listDatabases() {
		Assert.assertTrue(derby.listDatabases().contains(DERBY_SCHEMA.toUpperCase()));
	}

	@Test
	public void getDatabase() throws DatabaseNotExistException {
		Assert.assertNull(derby.getDatabase(DERBY_SCHEMA).getComment());
	}

	@Test
	public void databaseExists() {
		Assert.assertTrue(derby.databaseExists(DERBY_SCHEMA));
	}

	@Test
	public void createDatabase() throws DatabaseAlreadyExistException {
		derby.createDatabase(DERBY_SCHEMA_1, new CatalogDatabaseImpl(Collections.emptyMap(), ""), true);
		Assert.assertTrue(derby.databaseExists(DERBY_SCHEMA_1));
	}

	@Test
	public void dropDatabase()
		throws DatabaseNotEmptyException, DatabaseNotExistException, DatabaseAlreadyExistException {
		derby.createDatabase(DERBY_SCHEMA_1, new CatalogDatabaseImpl(Collections.emptyMap(), ""), true);
		derby.dropDatabase(DERBY_SCHEMA_1, true);
		Assert.assertFalse(derby.databaseExists(DERBY_SCHEMA_1));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterDatabase() throws DatabaseNotExistException {
		derby.alterDatabase(DERBY_SCHEMA_1, new CatalogDatabaseImpl(Collections.emptyMap(), ""), true);
	}

	@Test
	public void listTables() throws DatabaseNotExistException, TableAlreadyExistException, TableNotExistException {
		derby.createTable(
			new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE),
			new CatalogTableImpl(DERBY_DB_TABLE_SCHEMA, Collections.emptyMap(), ""),
			true
		);

		Assert.assertTrue(derby.listTables(DERBY_SCHEMA).contains(DERBY_DB_TABLE.toUpperCase()));
	}

	@Test
	public void listViews() throws DatabaseNotExistException {
		Assert.assertTrue(derby.listViews(DERBY_SCHEMA).isEmpty());
	}

	@Test(expected = TableNotExistException.class)
	public void getTable() throws TableNotExistException {
		derby.dropTable(new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE), true);
		derby.getTable(new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE));
	}

	@Test
	public void tableExists() {
		Assert.assertFalse(derby.tableExists(new ObjectPath(DERBY_DB, DERBY_DB_TABLE)));
	}

	@Test
	public void dropTable() throws TableNotExistException, TableAlreadyExistException, DatabaseNotExistException {
		derby.createTable(
			new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE),
			new CatalogTableImpl(DERBY_DB_TABLE_SCHEMA, Collections.emptyMap(), ""),
			true
		);

		Assert.assertTrue(derby.tableExists(new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE)));

		derby.dropTable(new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE), true);

		Assert.assertFalse(derby.tableExists(new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE)));
	}

	@Test
	public void renameTable() throws TableAlreadyExistException, TableNotExistException, DatabaseNotExistException {
		derby.createTable(
			new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE),
			new CatalogTableImpl(DERBY_DB_TABLE_SCHEMA, Collections.emptyMap(), ""),
			true
		);

		derby.renameTable(new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE), DERBY_DB_TABLE_1, true);

		Assert.assertTrue(derby.tableExists(new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE_1)));
	}

	@Test
	public void createTable() throws TableAlreadyExistException, DatabaseNotExistException {
		derby.createTable(
			new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE),
			new CatalogTableImpl(
				TableSchema.builder().fields(new String[] {"col0"}, new DataType[] {DataTypes.STRING()}).build(),
				Collections.emptyMap(), ""
			),
			true
		);

		Assert.assertTrue(derby.tableExists(new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE)));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterTable() throws TableNotExistException {
		derby.alterTable(new ObjectPath(DERBY_DB, DERBY_DB_TABLE), new CatalogTableImpl(
				TableSchema.builder().fields(new String[] {"col0"}, new DataType[] {DataTypes.STRING()}).build(),
				Collections.emptyMap(), ""
			),
			true);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void listPartitions() throws TableNotPartitionedException, TableNotExistException {
		derby.listPartitions(new ObjectPath(DERBY_DB, DERBY_DB_TABLE));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testListPartitions() throws TableNotPartitionedException, TableNotExistException {
		System.out.println(
			derby.listPartitions(
				new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
				new CatalogPartitionSpec(Collections.emptyMap())
			)
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getPartition() throws PartitionNotExistException {
		derby.getPartition(
			new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
			new CatalogPartitionSpec(Collections.emptyMap()));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void partitionExists() {
		System.out.println(
			derby.partitionExists(
				new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
				new CatalogPartitionSpec(Collections.emptyMap()))
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void createPartition()
		throws TableNotPartitionedException, PartitionAlreadyExistsException, TableNotExistException,
		PartitionSpecInvalidException {
		derby.createPartition(
			new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
			new CatalogPartitionSpec(Collections.emptyMap()),
			new CatalogPartitionImpl(Collections.emptyMap(), ""),
			true
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void dropPartition() throws PartitionNotExistException {
		derby.dropPartition(
			new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
			new CatalogPartitionSpec(Collections.emptyMap()),
			true
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterPartition() throws PartitionNotExistException {
		derby.alterPartition(
			new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
			new CatalogPartitionSpec(Collections.emptyMap()),
			new CatalogPartitionImpl(Collections.emptyMap(), ""),
			true
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void listFunctions() throws DatabaseNotExistException {
		derby.listFunctions(DERBY_DB);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getFunction() throws FunctionNotExistException {
		derby.getFunction(new ObjectPath(DERBY_DB, DERBY_DB_FUNCTION));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void functionExists() {
		derby.functionExists(new ObjectPath(DERBY_DB, DERBY_DB_FUNCTION));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void createFunction() throws FunctionAlreadyExistException, DatabaseNotExistException {
		derby.createFunction(
			new ObjectPath(DERBY_DB, DERBY_DB_FUNCTION),
			new CatalogFunctionImpl("test", FunctionLanguage.JAVA),
			true
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterFunction() throws FunctionNotExistException {
		derby.alterFunction(
			new ObjectPath(DERBY_DB, DERBY_DB_FUNCTION),
			new CatalogFunctionImpl("test", FunctionLanguage.JAVA),
			true
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void dropFunction() throws FunctionNotExistException {
		derby.dropFunction(
			new ObjectPath(DERBY_DB, DERBY_DB_FUNCTION),
			true
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getTableStatistics() throws TableNotExistException {
		derby.getTableStatistics(new ObjectPath(DERBY_DB, DERBY_DB_TABLE));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getTableColumnStatistics() throws TableNotExistException {
		derby.getTableColumnStatistics(new ObjectPath(DERBY_DB, DERBY_DB_TABLE));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getPartitionStatistics() throws PartitionNotExistException {
		derby.getPartitionStatistics(new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
			new CatalogPartitionSpec(Collections.emptyMap()));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void getPartitionColumnStatistics() throws PartitionNotExistException {
		derby.getPartitionColumnStatistics(new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
			new CatalogPartitionSpec(Collections.emptyMap()));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterTableStatistics() throws TableNotExistException {
		derby.alterTableStatistics(new ObjectPath(DERBY_DB, DERBY_DB_TABLE), new CatalogTableStatistics(0L, 0, 0L, 0L),
			true);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterTableColumnStatistics() throws TablePartitionedException, TableNotExistException {
		derby.alterTableColumnStatistics(
			new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
			new CatalogColumnStatistics(Collections.emptyMap()),
			true
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterPartitionStatistics() throws PartitionNotExistException {
		derby.alterPartitionStatistics(
			new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
			new CatalogPartitionSpec(Collections.emptyMap()),
			new CatalogTableStatistics(0L, 0, 0L, 0L),
			true
		);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void alterPartitionColumnStatistics() throws PartitionNotExistException {
		derby.alterPartitionColumnStatistics(
			new ObjectPath(DERBY_DB, DERBY_DB_TABLE),
			new CatalogPartitionSpec(Collections.emptyMap()),
			new CatalogColumnStatistics(Collections.emptyMap()),
			true
		);
	}

	@Test
	public void sinkStream() throws Exception {
		Row[] rows = new Row[] {
			Row.of(
				0L,
				"string",
				new Date(0),
				new BigDecimal("0.00"),
				0.0,
				0.0f,
				0,
				(short) 0,
				new Time(0),
				new Timestamp(0),
				new byte[] {0, 1}
			)
		};

		MemSourceStreamOp memSourceStreamOp = new MemSourceStreamOp(
			Arrays.asList(rows),
			new TableSchema(
				new String[] {
					"col_long", "col_string", "col_date",
					"col_bigdecimal", "col_double", "col_float",
					"col_int", "col_short", "col_time",
					"col_timestamp", "col_varcharforbit"
				},
				new TypeInformation <?>[] {
					Types.LONG, Types.STRING, Types.SQL_DATE,
					Types.BIG_DEC, Types.DOUBLE, Types.FLOAT, Types.INT,
					Types.SHORT, Types.SQL_TIME, Types.SQL_TIMESTAMP,
					Types.PRIMITIVE_ARRAY(Types.BYTE)
				}
			)
		);

		derby.sinkStream(
			new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE),
			memSourceStreamOp.getOutputTable(),
			new Params(),
			memSourceStreamOp.getMLEnvironmentId()
		);

		StreamOperator.execute();

		new TableSourceStreamOp(
			derby.sourceStream(new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE), new Params(),
				MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID)
		).print();

		StreamOperator.execute();
	}

	@Test
	public void sinkBatch() throws Exception {
		Row[] rows = new Row[] {
			Row.of(
				0L,
				"string",
				new Date(0),
				new BigDecimal("0.00"),
				0.0,
				0.0f,
				0,
				(short) 0,
				new Time(0),
				new Timestamp(0),
				new byte[] {0, 1}
			)
		};

		MemSourceBatchOp memSourceBatchOp = new MemSourceBatchOp(
			Arrays.asList(rows),
			new TableSchema(
				new String[] {
					"col_long", "col_string", "col_date",
					"col_bigdecimal", "col_double", "col_float",
					"col_int", "col_short", "col_time",
					"col_timestamp", "col_varcharforbit"
				},
				new TypeInformation <?>[] {
					Types.LONG, Types.STRING, Types.SQL_DATE,
					Types.BIG_DEC, Types.DOUBLE, Types.FLOAT, Types.INT,
					Types.SHORT, Types.SQL_TIME, Types.SQL_TIMESTAMP,
					Types.PRIMITIVE_ARRAY(Types.BYTE)
				}
			)
		);

		derby.sinkBatch(
			new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE),
			memSourceBatchOp.getOutputTable(),
			new Params(),
			memSourceBatchOp.getMLEnvironmentId()
		);

		BatchOperator.execute();

		Assert.assertFalse(
			new TableSourceBatchOp(
				derby.sourceBatch(new ObjectPath(DERBY_SCHEMA, DERBY_DB_TABLE), new Params(),
					MLEnvironmentFactory.DEFAULT_ML_ENVIRONMENT_ID)
			).collect().isEmpty()
		);
	}
}