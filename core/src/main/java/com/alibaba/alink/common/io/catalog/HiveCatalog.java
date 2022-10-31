package com.alibaba.alink.common.io.catalog;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
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
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.OutputFormatTableSink;
import org.apache.flink.table.sinks.OverwritableTableSink;
import org.apache.flink.table.sinks.PartitionableTableSink;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.PartitionableTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.CatalogAnnotation;
import com.alibaba.alink.common.io.catalog.HiveBaseUtils.HiveConfFolderStructure;
import com.alibaba.alink.common.io.catalog.plugin.HiveClassLoaderFactory;
import com.alibaba.alink.common.io.plugin.wrapper.RichInputFormatWithClassLoader;
import com.alibaba.alink.common.io.plugin.wrapper.RichOutputFormatWithClassLoader;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.HiveCatalogParams;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CatalogAnnotation(name = "hive")
public class HiveCatalog extends BaseCatalog {

	public static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

	private static final String CATALOG_HIVE_VERSION = "hive-version";
	private static final String CATALOG_HIVE_CONF_DIR = "hive-conf-dir";
	private static final String CATALOG_DEFAULT_DATABASE = "default-database";
	private Catalog internal;

	private final HiveClassLoaderFactory hiveClassLoaderFactory;

	public HiveCatalog(String catalogName, String defaultDatabase, String hiveVersion, String hiveConfDir) {
		this(catalogName, defaultDatabase, hiveVersion, FilePath.deserialize(hiveConfDir), null, null);
	}

	public HiveCatalog(String catalogName, String defaultDatabase, String hiveVersion, FilePath hiveConfDir) {
		this(catalogName, defaultDatabase, hiveVersion, hiveConfDir, null, null);
	}

	public HiveCatalog(
		String catalogName, String defaultDatabase, String hiveVersion, String hiveConfDir,
		String kerberosPrincipal, String kerberosKeytab) {

		this(catalogName, defaultDatabase, hiveVersion, new FilePath(hiveConfDir), kerberosPrincipal, kerberosKeytab);
	}

	public HiveCatalog(
		String catalogName, String defaultDatabase, String hiveVersion, FilePath hiveConfDir,
		String kerberosPrincipal, String kerberosKeytab) {

		this(new Params()
			.set(HiveCatalogParams.CATALOG_NAME, catalogName)
			.set(HiveCatalogParams.DEFAULT_DATABASE, defaultDatabase == null ? "default" : defaultDatabase)
			.set(HiveCatalogParams.HIVE_CONF_DIR, hiveConfDir.serialize())
			.set(HiveCatalogParams.PLUGIN_VERSION, hiveVersion)
			.set(HiveCatalogParams.KERBEROS_PRINCIPAL, kerberosPrincipal)
			.set(HiveCatalogParams.KERBEROS_KEYTAB, kerberosKeytab)
		);
	}

	public HiveCatalog(Params params) {
		super(params);

		hiveClassLoaderFactory = new HiveClassLoaderFactory(getParams().get(HiveCatalogParams.PLUGIN_VERSION),
			getParams());

		if (getParams().get(HiveCatalogParams.KERBEROS_PRINCIPAL) == null
			|| getParams().get(HiveCatalogParams.KERBEROS_KEYTAB) == null) {

			Preconditions.checkNotNull(
				getParams().get(HiveCatalogParams.HIVE_CONF_DIR),
				"Directory of hive configure should not be null"
			);

			HiveConfFolderStructure structure = new HiveConfFolderStructure(
				FilePath.deserialize(getParams().get(HiveCatalogParams.HIVE_CONF_DIR))
			);

			try {
				String principal = structure.getKerberosPrincipal();
				FilePath keytabPath = structure.getKerberosKeytabPath();

				getParams().set(HiveCatalogParams.KERBEROS_PRINCIPAL, principal);
				getParams().set(HiveCatalogParams.KERBEROS_KEYTAB, keytabPath == null ? null : keytabPath.serialize());
			} catch (IOException ignored) {
				// pass

				getParams().set(HiveCatalogParams.KERBEROS_PRINCIPAL, null);
				getParams().set(HiveCatalogParams.KERBEROS_KEYTAB, null);
			}
		}
	}

	@Override
	public void open() throws CatalogException {
		hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().open());
	}

	@Override
	public void close() throws CatalogException {
		hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().close());
	}

	@Override
	public List <String> listDatabases() throws CatalogException {
		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listDatabases());
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getDatabase(databaseName));
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().databaseExists(databaseName));
	}

	@Override
	public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
		throws DatabaseAlreadyExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().createDatabase(name, database, ignoreIfExists));
	}

	@Override
	public void dropDatabase(String name, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().dropDatabase(name, ignoreIfNotExists));
	}

	@Override
	public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
		throws DatabaseNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().alterDatabase(name, newDatabase, ignoreIfNotExists));
	}

	@Override
	public List <String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listTables(databaseName));
	}

	@Override
	public List <String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listViews(databaseName));
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getTable(tablePath));
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().tableExists(tablePath));
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().dropTable(tablePath, ignoreIfNotExists));
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().renameTable(tablePath, newTableName, ignoreIfNotExists));
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().createTable(tablePath, table, ignoreIfExists));
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().alterTable(tablePath, newTable,
			ignoreIfNotExists));
	}

	@Override
	public List <CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
		throws TableNotExistException, TableNotPartitionedException, CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listPartitions(tablePath));
	}

	@Override
	public List <CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws TableNotExistException, TableNotPartitionedException, CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listPartitions(tablePath, partitionSpec));
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getPartition(tablePath, partitionSpec));
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().partitionExists(tablePath, partitionSpec));
	}

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition,
								boolean ignoreIfExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
		PartitionAlreadyExistsException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().createPartition(tablePath, partitionSpec, partition, ignoreIfExists));
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().dropPartition(tablePath, partitionSpec, ignoreIfNotExists));
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition,
							   boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().alterPartition(tablePath, partitionSpec, newPartition, ignoreIfNotExists));
	}

	@Override
	public List <String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listFunctions(dbName));
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getFunction(functionPath));
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().functionExists(functionPath));
	}

	@Override
	public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
		throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().createFunction(functionPath, function, ignoreIfExists));
	}

	@Override
	public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
		throws FunctionNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().alterFunction(functionPath, newFunction, ignoreIfNotExists));
	}

	@Override
	public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
		throws FunctionNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().dropFunction(functionPath, ignoreIfNotExists));
	}

	@Override
	public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
		throws TableNotExistException, CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getTableStatistics(tablePath));
	}

	@Override
	public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
		throws TableNotExistException, CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().getTableColumnStatistics(tablePath));
	}

	@Override
	public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().getPartitionStatistics(tablePath, partitionSpec));
	}

	@Override
	public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
																CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException {

		return hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().getPartitionColumnStatistics(tablePath, partitionSpec));
	}

	@Override
	public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
									 boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists));
	}

	@Override
	public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics,
										   boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException, TablePartitionedException {

		hiveClassLoaderFactory.doAsThrowRuntime(
			() -> loadCatalog().alterTableColumnStatistics(tablePath, columnStatistics, ignoreIfNotExists));
	}

	@Override
	public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
										 CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog()
			.alterPartitionStatistics(tablePath, partitionSpec, partitionStatistics, ignoreIfNotExists));
	}

	@Override
	public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
											   CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException {

		hiveClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog()
			.alterPartitionColumnStatistics(tablePath, partitionSpec, columnStatistics, ignoreIfNotExists));
	}

	@Override
	public Table sourceStream(ObjectPath objectPath, Params params, Long sessionId) {
		Tuple3 <TableSchema, DataType, RichInputFormatWithClassLoader <Row>> all
			= hiveClassLoaderFactory.doAsThrowRuntime(() -> {

			InputFormatTableSource <Row> inputFormatTableSource;
			RichInputFormatWithClassLoader <Row> inputFormat;
			inputFormatTableSource = HiveCatalog.createHiveTableSource(objectPath, params, loadCatalog(),
				hiveClassLoaderFactory);
			inputFormat = new RichInputFormatWithClassLoader(
				hiveClassLoaderFactory,
				(RichInputFormat <Row, InputSplit>) inputFormatTableSource.getInputFormat()
			);

			TableSchema schema = inputFormatTableSource.getTableSchema();
			return Tuple3.of(schema, inputFormatTableSource.getProducedDataType(), inputFormat);

		});

		DataStream <Row> dataStream = MLEnvironmentFactory
			.get(sessionId)
			.getStreamExecutionEnvironment()
			.createInput(
				all.f2,
				(TypeInformation <Row>) TypeConversions.fromDataTypeToLegacyInfo(
					all.f1
				)
			);

		Table tbl = DataStreamConversionUtil.toTable(sessionId, dataStream, all.f0);

		try {
			if (getPartitionCols(objectPath).size() > 0) { // remove static partition columns
				String[] fieldNames = getTable(objectPath).getSchema().getFieldNames();
				tbl = tbl.select(Joiner.on(",").join(fieldNames));
			}
		} catch (TableNotExistException e) {
			throw new RuntimeException(e);
		}

		return tbl;
	}

	@Override
	public void sinkStream(ObjectPath objectPath, Table in, Params params, Long sessionId) {
		OutputFormat <Row> outputFormat =
			hiveClassLoaderFactory.doAsThrowRuntime(() -> {
					checkTableExistenceBeforeSink(objectPath, in.getSchema(), params);

					OutputFormatTableSink <Row> tableSink = HiveCatalog.createHiveTableSink(objectPath, params,
						loadCatalog(), hiveClassLoaderFactory);

					((PartitionableTableSink) tableSink).setStaticPartition(
						HiveBaseUtils.getStaticPartitionSpec(params.get(HiveCatalogParams.PARTITION)));

					((OverwritableTableSink) tableSink).setOverwrite(true);

					return tableSink.getOutputFormat();
				}
			);

		StreamOperator
			.fromTable(in)
			.setMLEnvironmentId(sessionId)
			.getDataStream()
			.writeUsingOutputFormat(
				new RichOutputFormatWithClassLoader(hiveClassLoaderFactory, outputFormat)
			).name("hive_stream_sink_" + objectPath.getFullName());
	}

	@Override
	public Table sourceBatch(ObjectPath objectPath, Params params, Long sessionId) {
		Tuple3 <TableSchema, DataType, RichInputFormatWithClassLoader <Row>> all =
			hiveClassLoaderFactory.doAsThrowRuntime(() -> {
				InputFormatTableSource <Row> inputFormatTableSource;
				RichInputFormatWithClassLoader <Row> inputFormat;
				inputFormatTableSource = createHiveTableSource(objectPath, params, loadCatalog(),
					hiveClassLoaderFactory);
				inputFormatTableSource.getProducedDataType();
				inputFormat = new RichInputFormatWithClassLoader(
					hiveClassLoaderFactory,
					(RichInputFormat <Row, InputSplit>) inputFormatTableSource.getInputFormat()
				);

				TableSchema schema = inputFormatTableSource.getTableSchema();
				return Tuple3.of(schema, inputFormatTableSource.getProducedDataType(), inputFormat);
			});

		DataSet <Row> ds = MLEnvironmentFactory.get(sessionId)
			.getExecutionEnvironment()
			.createInput(
				all.f2,
				(TypeInformation <Row>) TypeConversions.fromDataTypeToLegacyInfo(
					all.f1
				)
			);

		Table tbl = DataSetConversionUtil.toTable(sessionId, ds, all.f0);

		try {
			if (getPartitionCols(objectPath).size() > 0) {
				// remove static partition columns
				String[] fieldNames = getTable(objectPath).getSchema().getFieldNames();
				tbl = tbl.select(Joiner.on(",").join(fieldNames));
			}
		} catch (TableNotExistException e) {
			throw new RuntimeException(e);
		}

		return tbl;
	}

	@Override
	public void sinkBatch(ObjectPath objectPath, Table in, Params params, Long sessionId) {
		OutputFormat <Row> outputFormat = hiveClassLoaderFactory.doAsThrowRuntime(() -> {
				checkTableExistenceBeforeSink(objectPath, in.getSchema(), params);
				OutputFormatTableSink <Row> tableSink = HiveCatalog.createHiveTableSink(objectPath, params,
					loadCatalog(), hiveClassLoaderFactory);
				((PartitionableTableSink) tableSink).setStaticPartition(
					HiveBaseUtils.getStaticPartitionSpec(params.get(HiveCatalogParams.PARTITION)));
				((OverwritableTableSink) tableSink).setOverwrite(true);

				return tableSink.getOutputFormat();
			}
		);

		BatchOperator
			.fromTable(in)
			.setMLEnvironmentId(sessionId)
			.getDataSet()
			.output(
				new RichOutputFormatWithClassLoader(hiveClassLoaderFactory, outputFormat)
			).name("hive_batch_sink_" + objectPath.getFullName());
	}

	public List <String> getPartitionCols(ObjectPath objectPath) throws TableNotExistException {
		return ((CatalogTable) getTable(objectPath)).getPartitionKeys();
	}

	private Catalog loadCatalog() {
		if (internal == null) {
			internal = hiveClassLoaderFactory
				.doAsThrowRuntime(() -> {
					Catalog catalog = createCatalog(getParams(), Thread.currentThread().getContextClassLoader());
					catalog.open();

					return catalog;
				});
		}

		return internal;
	}

	private void checkTableExistenceBeforeSink(ObjectPath objectPath, TableSchema schema, Params params) {
		boolean tableExists = tableExists(objectPath);
		boolean overwriteSink = params.get(HiveCatalogParams.OVERWRITE_SINK);

		if (tableExists) {
			if (overwriteSink) {
				checkSchemaMatch(schema, objectPath);
			} else {
				throw new RuntimeException(String.format(
					"Table %s exists, you may setOverwriteSink(true) to overwrite this table.",
					objectPath.getFullName()));
			}
		} else {
			try {
				createTable(objectPath, createNewTableDesc(objectPath, schema, params), false);
			} catch (Exception e) {
				LOG.warn("Failed to create table {}", objectPath.getFullName(), e);
				throw new RuntimeException("Fail to create table " + objectPath.getFullName(), e);
			}
		}
	}

	private void checkSchemaMatch(TableSchema outputSchema, ObjectPath objectPath) {
		TableSchema tableSchema;
		try {
			tableSchema = getTable(objectPath).getSchema();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		String[] outputFields = outputSchema.getFieldNames();
		String[] tableFields = tableSchema.getFieldNames();

		if (outputFields.length != tableFields.length) {
			throw new RuntimeException(
				String.format("mismatched schema size between outputting operator and the overwritten table \"%s\"." +
					"Outputting operator schema is %s", objectPath.getFullName(), outputSchema.toString()));
		}

		for (int i = 0; i < outputFields.length; i++) {
			if (outputFields[i].compareToIgnoreCase(tableFields[i]) != 0) {
				throw new RuntimeException(
					String.format("mismatched schema between outputting operator and the overwritten table \"%s\"." +
						"Outputting operator schema is %s", objectPath.getFullName(), outputSchema.toString()));
			}
		}
	}

	private static CatalogBaseTable createNewTableDesc(ObjectPath objectPath, TableSchema schema, Params params) {
		String[] partitionCols = new String[0];
		String partitionSpec = params.get(HiveCatalogParams.PARTITION);

		if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
			partitionCols = partitionSpec.split("/");
			for (int i = 0; i < partitionCols.length; i++) {
				String p = partitionCols[i];
				int pos = p.indexOf('=');
				Preconditions.checkArgument(pos > 0);
				partitionCols[i] = p.substring(0, pos);
			}
		}
		if (partitionCols.length > 0) { // create table with static partition columns
			for (String c : partitionCols) {
				if (TableUtil.findColIndex(schema, c) >= 0) {
					throw new IllegalArgumentException("The table contains partition column: " + c);
				}
			}

			String[] fieldNames = ArrayUtils.addAll(schema.getFieldNames(), partitionCols);
			TypeInformation <?>[] fieldTypes = new TypeInformation[partitionCols.length];
			Arrays.fill(fieldTypes, Types.STRING);
			fieldTypes = ArrayUtils.addAll(schema.getFieldTypes(), fieldTypes);
			schema = new TableSchema(fieldNames, fieldTypes);
		}

		Map <String, String> properties = new HashMap <>();
		properties.put(CatalogConfig.IS_GENERIC, "false");

		return new CatalogTableImpl(schema, Arrays.asList(partitionCols), properties, objectPath.getFullName());
	}

	public static CatalogFactory createCatalogFactory(ClassLoader classLoader) {
		try {
			return (CatalogFactory) classLoader
				.loadClass("org.apache.flink.table.catalog.hive.factories.HiveCatalogFactory")
				.getConstructor()
				.newInstance();
		} catch (ClassNotFoundException | NoSuchMethodException
			| InstantiationException | IllegalAccessException | InvocationTargetException e) {

			throw new RuntimeException("Could not find the hive catelog factory.", e);
		}
	}

	public static Catalog createCatalog(Params params, ClassLoader classLoader) {
		String catalogName = params.get(HiveCatalogParams.CATALOG_NAME);

		CatalogFactory factory = createCatalogFactory(classLoader);

		List <String> supportedKeys = factory.supportedProperties();

		if (!supportedKeys.contains(CATALOG_HIVE_VERSION)
			|| !supportedKeys.contains(CATALOG_HIVE_CONF_DIR)
			|| !supportedKeys.contains(CATALOG_DEFAULT_DATABASE)) {

			throw new IllegalStateException(
				"Incorrect hive dependency. Please check the configure of hive environment."
			);
		}

		String localHiveConfDir;

		try {
			localHiveConfDir = HiveBaseUtils.downloadHiveConf(
				FilePath.deserialize(params.get(HiveCatalogParams.HIVE_CONF_DIR))
			);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}

		Map <String, String> properties = new HashMap <>();

		properties.put(CATALOG_HIVE_VERSION, params.get(HiveCatalogParams.PLUGIN_VERSION));
		properties.put(CATALOG_HIVE_CONF_DIR, localHiveConfDir);

		if (params.get(HiveCatalogParams.DEFAULT_DATABASE) != null) {
			properties.put(CATALOG_DEFAULT_DATABASE, params.get(HiveCatalogParams.DEFAULT_DATABASE));
		}

		properties.putAll(factory.requiredContext());

		return factory.createCatalog(catalogName, properties);
	}

	public static List <Map <String, String>> getSelectedPartitions(String[] partitionSpecs) {
		List <Map <String, String>> selected = new ArrayList <>();
		for (String s : partitionSpecs) {
			Map <String, String> spec = HiveBaseUtils.getStaticPartitionSpec(s);
			selected.add(spec);
		}
		return selected;
	}

	public static CatalogTable getCatalogTable(ObjectPath objectPath, Catalog catalog,
											   HiveClassLoaderFactory action) {
		return (CatalogTable) action.doAsThrowRuntime(() -> catalog.getTable(objectPath));
	}

	public static <T> InputFormatTableSource <T> createHiveTableSource(
		ObjectPath objectPath, Params params, Catalog catalog, HiveClassLoaderFactory action) {
		return action.doAsThrowRuntime(() -> {
			InputFormatTableSource <T> tableSource = (InputFormatTableSource <T>) ((TableSourceFactory <T>) catalog
				.getTableFactory()
				.orElseGet(() -> {
					throw new RuntimeException("Could not create the table source factory in hive.");
				}))
				.createTableSource(objectPath, getCatalogTable(objectPath, catalog, action));

			String partitionSpecsStr = params.get(HiveCatalogParams.PARTITIONS);

			if (!StringUtils.isNullOrWhitespaceOnly(partitionSpecsStr)) {
				String[] partitionSpecs = partitionSpecsStr.split(",");
				// FIXME: This is a workaround to a bug in HiveTableSource in Flink 1.9
				((PartitionableTableSource) tableSource).getPartitions();
				List <Map <String, String>> selectedPartitions = getSelectedPartitions(partitionSpecs);
				tableSource = (InputFormatTableSource <T>) ((PartitionableTableSource) tableSource)
					.applyPartitionPruning(selectedPartitions);
			}

			return tableSource;
		});
	}

	public static OutputFormatTableSink <Row> createHiveTableSink(
		ObjectPath objectPath, Params params, Catalog catalog, HiveClassLoaderFactory action) {
		try {
			return action.doAs(() -> (OutputFormatTableSink <Row>) ((TableSinkFactory <Row>) catalog
				.getTableFactory()
				.orElseGet(() -> {
					throw new RuntimeException("Could not create the table sink factory in hive.");
				}))
				.createTableSink(objectPath, getCatalogTable(objectPath, catalog, action))
			);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
