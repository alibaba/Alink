package com.alibaba.alink.common.io.catalog;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
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
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.utils.DataSetConversionUtil;
import com.alibaba.alink.operator.stream.utils.DataStreamConversionUtil;
import com.alibaba.alink.params.shared.HasOverwriteSink;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

public abstract class JdbcCatalog extends BaseCatalog {

	public static final DataType LEGACY_DEC_DATA_TYPE = TypeConversions
		.fromLegacyInfoToDataType(org.apache.flink.api.common.typeinfo.Types.BIG_DEC);

	protected transient Connection connection;

	public JdbcCatalog(Params params) {
		super(params);
	}

	@Override
	public void close() throws CatalogException {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException ex) {
				throw new CatalogException(ex);
			}
		}
	}

	protected void executeSql(String sql) throws SQLException {
		try (Statement statement = connection.createStatement();) {
			statement.execute(sql);
		}
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName)
		throws DatabaseNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public List <String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List <String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public List <CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
		throws TableNotExistException, TableNotPartitionedException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public List <CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws TableNotExistException, TableNotPartitionedException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition,
								boolean ignoreIfExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
		PartitionAlreadyExistsException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition,
							   boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public List <String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
		throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
		throws FunctionNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
		throws FunctionNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
		throws TableNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
		throws TableNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
																CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
									 boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics,
										   boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException, TablePartitionedException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
										 CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
											   CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public Table sourceStream(ObjectPath objectPath, Params params, Long sessionId) {

		TableSchema schema;
		InputFormat <Row, InputSplit> inputFormat;

		try {
			schema = getTable(objectPath).getSchema();
			inputFormat = createInputFormat(objectPath, schema);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return DataStreamConversionUtil.toTable(
			sessionId,
			MLEnvironmentFactory
				.get(sessionId)
				.getStreamExecutionEnvironment()
				.createInput(inputFormat, new RowTypeInfo(schema.getFieldTypes()))
				.setParallelism(1),
			schema.getFieldNames(),
			schema.getFieldTypes()
		);
	}

	@Override
	public void sinkStream(ObjectPath objectPath, Table in, Params params, Long sessionId) {
		if (!tableExists(objectPath)) {
			try {
				createTable(objectPath, new CatalogTableImpl(in.getSchema(), Collections.emptyMap(), ""), true);
			} catch (TableAlreadyExistException | DatabaseNotExistException ex) {
				throw new CatalogException("Fail to create table: " + objectPath.toString(), ex);
			}
		}

		TableSchema schema = in.getSchema();
		String[] colNames = schema.getFieldNames();

		StringBuilder sbd = new StringBuilder();

		sbd.append("INSERT INTO ")
			.append(rewriteObjectPath(objectPath).getFullName())
			.append(" (")
			.append(colNames[0]);

		for (int i = 1; i < colNames.length; i++) {
			sbd.append(",").append(colNames[i]);
		}

		sbd.append(") VALUES (?");

		for (int i = 1; i < colNames.length; i++) {
			sbd.append(",").append("?");
		}

		sbd.append(")");

		String sql = sbd.toString();

		OutputFormat <Row> jdbcAppendTableSink = createOutputFormat(objectPath, schema, sql);

		MLEnvironmentFactory.get(sessionId)
			.getStreamTableEnvironment()
			.toAppendStream(in, new RowTypeInfo(in.getSchema().getFieldTypes()))
			.writeUsingOutputFormat(jdbcAppendTableSink);
	}

	@Override
	public Table sourceBatch(ObjectPath objectPath, Params params, Long sessionId) {
		TableSchema schema;
		InputFormat <Row, InputSplit> inputFormat;
		try {
			schema = getTable(objectPath).getSchema();
			inputFormat = createInputFormat(objectPath, schema);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}

		return DataSetConversionUtil.toTable(
			sessionId,
			MLEnvironmentFactory
				.get(sessionId)
				.getExecutionEnvironment()
				.createInput(inputFormat, new RowTypeInfo(schema.getFieldTypes()))
				.setParallelism(1),
			schema
		);
	}

	@Override
	public void sinkBatch(ObjectPath objectPath, Table in, Params params, Long sessionId) {

		boolean isOverwriteSink = params.get(HasOverwriteSink.OVERWRITE_SINK);

		//Create Table
		try {
			if (isOverwriteSink) {
				if (tableExists(objectPath)) {
					dropTable(objectPath, true);
				}
			}

			createTable(objectPath, new CatalogTableImpl(in.getSchema(), Collections.emptyMap(), ""), false);

		} catch (TableNotExistException | TableAlreadyExistException | DatabaseNotExistException ex) {
			throw new CatalogException(ex);
		}

		TableSchema schema = in.getSchema();
		String[] colNames = schema.getFieldNames();

		StringBuilder sbd = new StringBuilder();

		sbd.append("INSERT INTO ")
			.append(rewriteObjectPath(objectPath).getFullName())
			.append(" (")
			.append(colNames[0]);

		for (int i = 1; i < colNames.length; i++) {
			sbd.append(",").append(colNames[i]);
		}

		sbd.append(") VALUES (?");

		for (int i = 1; i < colNames.length; i++) {
			sbd.append(",").append("?");
		}

		sbd.append(")");

		String sql = sbd.toString();

		OutputFormat <Row> jdbcAppendTableSink = createOutputFormat(objectPath, schema, sql);

		BatchOperator.fromTable(in).setMLEnvironmentId(sessionId).getDataSet().output(jdbcAppendTableSink);
	}

	protected abstract int flinkType2JdbcType(DataType flinkType);

	protected int[] flinkTypes2JdbcTypes(DataType[] flinkTypes) {
		int[] jdbcTypes = new int[flinkTypes.length];

		for (int i = 0; i < flinkTypes.length; ++i) {
			jdbcTypes[i] = flinkType2JdbcType(flinkTypes[i]);
		}

		return jdbcTypes;
	}

	protected ObjectPath rewriteObjectPath(ObjectPath objectPath) {
		return objectPath;
	}

	protected String rewriteDbUrl(String url, ObjectPath objectPath) {
		return url;
	}

	protected abstract RichInputFormat <Row, InputSplit> createInputFormat(
		ObjectPath objectPath, TableSchema schema) throws Exception;

	protected abstract OutputFormat <Row> createOutputFormat(
		ObjectPath objectPath, TableSchema schema, String sql);
}
