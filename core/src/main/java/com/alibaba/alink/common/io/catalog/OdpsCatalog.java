package com.alibaba.alink.common.io.catalog;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.CatalogFactory.Context;
import org.apache.flink.table.factories.FactoryUtil.DefaultCatalogContext;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.annotations.CatalogAnnotation;
import com.alibaba.alink.common.io.catalog.plugin.OdpsClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.wrapper.RichInputFormatWithClassLoader;
import com.alibaba.alink.common.io.plugin.wrapper.RichOutputFormatWithClassLoader;
import com.alibaba.alink.params.io.OdpsCatalogParams;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CatalogAnnotation(name = "odps")
public class OdpsCatalog extends InputOutputFormatCatalog {
	public static final String CATALOG_TYPE_VALUE_ODPS = "odps";
	public static final String CATALOG_ODPS_ACCESS_ID = "accessId";
	public static final String CATALOG_ODPS_ACCESS_KEY = "accessKey";
	public static final String CATALOG_ODPS_PROJECT = "project";
	public static final String CATALOG_ODPS_ENDPOINT = "endpoint";
	public static final String CATALOG_ODPS_RUNNING_PROJECT = "runningProject";

	private final OdpsClassLoaderFactory classLoaderFactory;
	private InputOutputFormatCatalog internal;

	public OdpsCatalog(
		String catalogName, String defaultDatabase,
		String odpsVersion, String accessId, String accessKey,
		String project, String endPoint, String runningProject) {
		this(
			new Params()
				.set(OdpsCatalogParams.CATALOG_NAME, catalogName == null ? genRandomCatalogName() : catalogName)
				.set(OdpsCatalogParams.DEFAULT_DATABASE, defaultDatabase == null ? "default" : defaultDatabase)
				.set(OdpsCatalogParams.ACCESS_ID, accessId)
				.set(OdpsCatalogParams.ACCESS_KEY, accessKey)
				.set(OdpsCatalogParams.PROJECT, project)
				.set(OdpsCatalogParams.END_POINT, endPoint)
				.set(OdpsCatalogParams.RUNNING_PROJECT, runningProject)
				.set(OdpsCatalogParams.PLUGIN_VERSION, odpsVersion)
		);
	}

	public OdpsCatalog(Params params) {
		super(params);

		classLoaderFactory = new OdpsClassLoaderFactory(
			getParams().get(OdpsCatalogParams.PLUGIN_VERSION)
		);
	}

	private InputOutputFormatCatalog loadCatalog() {
		if (internal == null) {
			internal = classLoaderFactory.doAsThrowRuntime(
				() -> createCatalog(getParams(), Thread.currentThread().getContextClassLoader())
			);
		}

		return internal;
	}

	@Override
	protected RichInputFormat <Row, InputSplit> createInputFormat(ObjectPath objectPath, TableSchema schema,
																  Params params) throws Exception {
		return new RichInputFormatWithClassLoader <>(classLoaderFactory,
			loadCatalog().createInputFormat(objectPath, schema, params));
	}

	@Override
	protected OutputFormat <Row> createOutputFormat(ObjectPath objectPath, TableSchema schema, Params params) {
		return new RichOutputFormatWithClassLoader(classLoaderFactory,
			loadCatalog().createOutputFormat(objectPath, schema, params));
	}

	@Override
	public void open() throws CatalogException {
		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().open());
	}

	@Override
	public void close() throws CatalogException {
		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().close());
	}

	@Override
	public List <String> listDatabases() throws CatalogException {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listDatabases());
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getDatabase(databaseName));
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().databaseExists(databaseName));
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().createDatabase(name, database, ignoreIfExists));
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
		throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().dropDatabase(name, ignoreIfNotExists, cascade));
	}

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().alterDatabase(name, newDatabase, ignoreIfNotExists));
	}

	@Override
	public List <String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listTables(databaseName));
	}

	@Override
	public List <String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listViews(databaseName));
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getTable(tablePath));
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().tableExists(tablePath));
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().dropTable(tablePath, ignoreIfNotExists));
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().renameTable(tablePath, newTableName, ignoreIfNotExists));
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().createTable(tablePath, table, ignoreIfExists));
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().alterTable(tablePath, newTable, ignoreIfNotExists));
	}

	@Override
	public List <CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
		throws TableNotExistException, TableNotPartitionedException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listPartitions(tablePath));
	}

	@Override
	public List <CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listPartitions(tablePath, partitionSpec));
	}

	@Override
	public List <CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List <Expression> filters)
		throws TableNotExistException, TableNotPartitionedException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listPartitionsByFilter(tablePath, filters));
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getPartition(tablePath, partitionSpec));
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {

		return classLoaderFactory.doAsThrowRuntime(() -> loadCatalog().partitionExists(tablePath, partitionSpec));
	}

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition,
								boolean ignoreIfExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
		PartitionAlreadyExistsException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().createPartition(
				tablePath, partitionSpec, partition, ignoreIfExists
			)
		);
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().dropPartition(
				tablePath, partitionSpec, ignoreIfNotExists
			)
		);
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition,
							   boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().alterPartition(
				tablePath, partitionSpec, newPartition, ignoreIfNotExists
			)
		);
	}

	@Override
	public List <String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
		return classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().listFunctions(dbName)
		);
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
		return classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().getFunction(functionPath)
		);
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {
		return classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().functionExists(functionPath)
		);
	}

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
		throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().createFunction(functionPath, function, ignoreIfExists)
		);
	}

	@Override
	public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
		throws FunctionNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().alterFunction(functionPath, newFunction, ignoreIfNotExists)
		);
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
		throws FunctionNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().dropFunction(functionPath, ignoreIfNotExists)
		);
	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
		throws TableNotExistException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().getTableStatistics(tablePath)
		);
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
		throws TableNotExistException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().getTableColumnStatistics(tablePath)
		);
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().getPartitionStatistics(tablePath, partitionSpec)
		);
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
																CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException {

		return classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().getPartitionColumnStatistics(tablePath, partitionSpec)
		);
	}

	@Override
	public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
									 boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists)
		);
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics,
										   boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException, TablePartitionedException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().alterTableColumnStatistics(tablePath, columnStatistics, ignoreIfNotExists)
		);
	}

	@Override
	public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
										 CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog()
				.alterPartitionStatistics(tablePath, partitionSpec, partitionStatistics, ignoreIfNotExists)
		);
	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
											   CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException {

		classLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog()
				.alterPartitionColumnStatistics(tablePath, partitionSpec, columnStatistics, ignoreIfNotExists)
		);
	}

	public static CatalogFactory createCatalogFactory(ClassLoader classLoader) {
		try {
			return (CatalogFactory) classLoader
				.loadClass("com.alibaba.alink.common.io.catalog.odps.factories.OdpsCatalogFactory")
				.getConstructor()
				.newInstance();
		} catch (ClassNotFoundException | NoSuchMethodException
			| InstantiationException | IllegalAccessException | InvocationTargetException e) {

			throw new RuntimeException("Could not find the odps catalog factory.", e);
		}
	}

	private static InputOutputFormatCatalog createCatalog(Params params, ClassLoader classLoader) {
		String catalogName = params.get(OdpsCatalogParams.CATALOG_NAME);

		CatalogFactory factory = createCatalogFactory(classLoader);

		Map <String, String> properties = new HashMap <>();

		properties.put(CATALOG_ODPS_ACCESS_ID, params.get(OdpsCatalogParams.ACCESS_ID));
		properties.put(CATALOG_ODPS_ACCESS_KEY, params.get(OdpsCatalogParams.ACCESS_KEY));
		properties.put(CATALOG_ODPS_ENDPOINT, params.get(OdpsCatalogParams.END_POINT));
		properties.put(CATALOG_ODPS_PROJECT, params.get(OdpsCatalogParams.PROJECT));

		if (params.get(OdpsCatalogParams.RUNNING_PROJECT) != null) {
			properties.put(CATALOG_ODPS_RUNNING_PROJECT, params.get(OdpsCatalogParams.RUNNING_PROJECT));
		}

		Context context = new DefaultCatalogContext(catalogName, properties, null, null);

		return (InputOutputFormatCatalog) factory.createCatalog(context);
	}
}
