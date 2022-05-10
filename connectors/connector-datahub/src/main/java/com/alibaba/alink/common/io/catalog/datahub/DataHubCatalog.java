package com.alibaba.alink.common.io.catalog.datahub;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
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
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.catalog.SourceSinkFunctionCatalog;
import com.alibaba.alink.common.io.catalog.datahub.datastream.sink.DatahubPublicSinkFunction;
import com.alibaba.alink.common.io.catalog.datahub.datastream.source.DatahubPublicSourceFunction;
import com.alibaba.alink.common.io.catalog.datahub.datastream.util.DatahubClientProvider;
import com.alibaba.alink.params.io.DataHubParams;
import com.alibaba.alink.params.io.shared.HasCatalogName;
import com.alibaba.alink.params.io.shared.HasDefaultDatabase;
import com.aliyun.datahub.client.exception.ResourceNotFoundException;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.GetProjectResult;
import com.aliyun.datahub.client.model.GetTopicResult;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.RecordType;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class DataHubCatalog extends SourceSinkFunctionCatalog {

	private final DatahubClientProvider provider;

	public DataHubCatalog(String accessId, String accessKey, String project, String endPoint) {
		this(
			new Params()
				.set(DataHubParams.ACCESS_ID, accessId)
				.set(DataHubParams.ACCESS_KEY, accessKey)
				.set(DataHubParams.PROJECT, project)
				.set(DataHubParams.END_POINT, endPoint)
				.set(HasCatalogName.CATALOG_NAME, "datahub_catalog")
				.set(HasDefaultDatabase.DEFAULT_DATABASE, "datahub_default_database")
		);
	}

	private DatahubClientProvider createProvider(
		String endpoint,
		String accessId,
		String accessKey) {

		return new DatahubClientProvider(endpoint, accessId, accessKey);
	}

	public DataHubCatalog(Params params) {
		super(params);

		provider = createProvider(
			params.get(DataHubParams.END_POINT),
			params.get(DataHubParams.ACCESS_ID),
			params.get(DataHubParams.ACCESS_KEY)
		);
	}

	@Override
	protected RichSinkFunction <Row> createSinkFunction(ObjectPath objectPath, TableSchema schema, Params params) {
		return new DatahubPublicSinkFunction(
			params.get(DataHubParams.END_POINT),
			objectPath.getDatabaseName(),
			objectPath.getObjectName(),
			params.get(DataHubParams.ACCESS_ID),
			params.get(DataHubParams.ACCESS_KEY),
			new RowTypeInfo(schema.getFieldTypes())
		);
	}

	@Override
	protected RichParallelSourceFunction <Row> createSourceFunction(
		ObjectPath objectPath, TableSchema schema, Params params)
		throws Exception {

		return new DatahubPublicSourceFunction(
			params.get(DataHubParams.END_POINT),
			objectPath.getDatabaseName(),
			objectPath.getObjectName(),
			params.get(DataHubParams.ACCESS_ID),
			params.get(DataHubParams.ACCESS_KEY),
			getStartTime(params.get(DataHubParams.START_TIME)),
			getEndTime(params.get(DataHubParams.END_TIME))
		);
	}

	public static long parseDateStringToMs(String dateStr, String dateFormat) {
		try {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
			return simpleDateFormat.parse(dateStr).getTime();
		} catch (Exception e) {
			throw new RuntimeException("Fail to parse date string: " + dateStr);
		}
	}

	public static long getStartTime(String timeStr) {

		if (timeStr == null) {
			return new Date().getTime();
		} else {
			return parseDateStringToMs(timeStr, "yyyy-MM-dd HH:mm:ss");
		}
	}

	public static long getEndTime(String timeStr) {
		if (timeStr == null) {
			return Long.MAX_VALUE;
		} else {
			return parseDateStringToMs(timeStr, "yyyy-MM-dd HH:mm:ss");
		}
	}

	@Override
	public void open() throws CatalogException {
		// pass
	}

	@Override
	public void close() throws CatalogException {
		// pass
	}

	@Override
	public List <String> listDatabases() throws CatalogException {
		return provider.getClient().listProject().getProjectNames();
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		GetProjectResult getProjectResult = provider.getClient().getProject(databaseName);

		return new CatalogDatabaseImpl(new HashMap <>(), getProjectResult.getComment());
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		try {
			provider.getClient().getProject(databaseName);
			return true;
		} catch (ResourceNotFoundException resourceNotFoundException) {
			return false;
		}
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException, CatalogException {
		boolean exist = databaseExists(name);

		if (exist && ignoreIfExists) {
			return;
		}

		if (exist) {
			throw new DatabaseAlreadyExistException(getName(), getDefaultDatabase());
		}

		provider.getClient().createProject(name, database.getComment());
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
		boolean exist = databaseExists(name);

		if (!exist && ignoreIfNotExists) {
			return;
		}

		if (!exist) {
			throw new DatabaseNotExistException(getName(), getDefaultDatabase());
		}

		provider.getClient().deleteProject(name);
	}

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public List <String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		return provider.getClient().listTopic(databaseName).getTopicNames();
	}

	@Override
	public List <String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		GetTopicResult getTopicResult = provider.getClient().getTopic(
			tablePath.getDatabaseName(), tablePath.getObjectName()
		);

		RecordSchema recordSchema = getTopicResult.getRecordSchema();

		int length = recordSchema.getFields().size();

		String[] colNames = new String[length];
		TypeInformation <?>[] colTypes = new TypeInformation <?>[length];

		for (int i = 0; i < length; ++i) {
			colNames[i] = recordSchema.getField(i).getName();
			colTypes[i] = DatahubPublicSourceFunction.datahubTypeToFlinkType(recordSchema.getField(i).getType());
		}

		return new CatalogTableImpl(
			new TableSchema(colNames, colTypes),
			Collections.emptyMap(),
			getTopicResult.getComment()
		);
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		try {
			provider.getClient().getTopic(tablePath.getDatabaseName(), tablePath.getObjectName());
			return true;
		} catch (ResourceNotFoundException exception) {
			return false;
		}
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {
		boolean exists = tableExists(tablePath);

		if (!exists && ignoreIfNotExists) {
			return;
		}

		if (!exists) {
			throw new TableNotExistException(getName(), tablePath);
		}

		provider.getClient().deleteTopic(tablePath.getDatabaseName(), tablePath.getObjectName());
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

		boolean exists = tableExists(tablePath);

		if (exists && ignoreIfExists) {
			return;
		}

		if (exists) {
			throw new TableAlreadyExistException(getName(), tablePath);
		}

		TableSchema tableSchema = table.getSchema();

		RecordSchema recordSchema = new RecordSchema();

		for (int i = 0; i < tableSchema.getFieldTypes().length; ++i) {
			recordSchema.addField(
				new Field(
					tableSchema.getFieldNames()[i],
					DatahubPublicSourceFunction.flinkTypeToDatahubType(tableSchema.getFieldTypes()[i])
				)
			);
		}

		String comment = table.getComment();

		String sharedCountStr = table.getProperties().get("shareCount");

		if (sharedCountStr == null) {
			sharedCountStr = "1";
		}

		int sharedCount = Integer.parseInt(sharedCountStr);

		String lifeCycleStr = table.getProperties().get("lifeCycle");

		if (lifeCycleStr == null) {
			lifeCycleStr = "3";
		}

		int lifeCycle = Integer.parseInt(lifeCycleStr);

		provider.getClient().createTopic(
			tablePath.getDatabaseName(),
			tablePath.getObjectName(),
			sharedCount,
			lifeCycle,
			RecordType.TUPLE,
			recordSchema,
			comment
		);
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
		return null;
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
}
