package com.alibaba.alink.common.io.catalog.odps;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.alibaba.alink.common.io.catalog.InputOutputFormatCatalog;
import com.alibaba.alink.common.io.catalog.odps.ElementSplitUtil.ElementSegment;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.params.io.OdpsCatalogParams;
import com.alibaba.alink.params.io.OdpsSinkParams;
import com.alibaba.alink.params.io.OdpsSourceParams;
import com.alibaba.alink.params.io.shared.HasCatalogName;
import com.alibaba.alink.params.io.shared.HasDefaultDatabase;
import com.aliyun.odps.Column;
import com.aliyun.odps.Function;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public final class OdpsCatalog extends InputOutputFormatCatalog {

	private transient Odps odps = null;

	public OdpsCatalog(Params params) {
		super(params);
	}

	public OdpsCatalog(String accessId, String accessKey, String project, String endPoint, String runningProject) {
		super(
			new Params()
				.set(OdpsCatalogParams.ACCESS_ID, accessId)
				.set(OdpsCatalogParams.ACCESS_KEY, accessKey)
				.set(OdpsCatalogParams.PROJECT, project)
				.set(OdpsCatalogParams.END_POINT, endPoint)
				.set(HasCatalogName.CATALOG_NAME, "odps_catalog")
				.set(HasDefaultDatabase.DEFAULT_DATABASE, "odps_default_database")
		);

		if (runningProject != null && !runningProject.isEmpty()) {
			getParams().set(OdpsCatalogParams.RUNNING_PROJECT, runningProject);
		}
	}

	private Odps getCurrentOdps() {
		if (odps == null) {
			odps = new Odps(
				new AliyunAccount(
					getParams().get(OdpsCatalogParams.ACCESS_ID),
					getParams().get(OdpsCatalogParams.ACCESS_KEY)
				)
			);

			odps.setEndpoint(getParams().get(OdpsCatalogParams.END_POINT));

			if (getParams().contains(OdpsCatalogParams.RUNNING_PROJECT)) {
				odps.setDefaultProject(getParams().get(OdpsCatalogParams.RUNNING_PROJECT));
			} else {
				odps.setDefaultProject(getParams().get(OdpsCatalogParams.PROJECT));
			}
		}
		return odps;
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
		Iterator <Project> projects = getCurrentOdps().projects().iterator(null);
		List <String> projectList = new ArrayList <>();
		projects.forEachRemaining(project -> projectList.add(project.getName()));
		return projectList;
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		Project project;

		try {
			project = getCurrentOdps().projects().get(databaseName);
		} catch (OdpsException e) {
			throw new CatalogException(e);
		}

		return new CatalogDatabaseImpl(new HashMap <>(), project.getComment());
	}

	@Override
	public boolean databaseExists(String databaseName) throws CatalogException {
		try {
			return getCurrentOdps().projects().exists(databaseName);
		} catch (OdpsException e) {
			throw new CatalogException(e);
		}
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

		if (ignoreIfNotExists && !databaseExists(name)) {
			return;
		}

		try {
			getCurrentOdps().projects().updateProject(name, newDatabase.getProperties());
		} catch (OdpsException e) {
			throw new CatalogException(e);
		}
	}

	@Override
	public List <String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		List <String> tables = new ArrayList <>();

		getCurrentOdps().tables().iterable(databaseName).forEach(table -> tables.add(table.getName()));

		return tables;
	}

	@Override
	public List <String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
		List <String> tables = new ArrayList <>();

		getCurrentOdps().tables().iterable(databaseName).forEach(table -> {
			if (table.isVirtualView()) {
				tables.add(table.getName());
			}
		});

		return tables;
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}

		com.aliyun.odps.Table odpsTable = getCurrentOdps().tables().get(
			tablePath.getDatabaseName(), tablePath.getObjectName()
		);

		List <Column> columns = odpsTable.getSchema().getColumns();

		return new CatalogTableImpl(
			new TableSchema(
				OdpsTableUtil.getColNames(columns),
				OdpsTableUtil.getColTypes(columns)
			),
			new HashMap <>(),
			odpsTable.getComment()
		);
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		try {
			return getCurrentOdps().tables().exists(tablePath.getDatabaseName(), tablePath.getObjectName());
		} catch (OdpsException e) {
			throw new CatalogException(e);
		}
	}

	@Override
	public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {
		try {
			getCurrentOdps().tables().delete(
				tablePath.getDatabaseName(),
				tablePath.getObjectName(),
				ignoreIfNotExists
			);
		} catch (OdpsException e) {
			throw new CatalogException(e);
		}
	}

	public void execute(String sql) throws Exception {
		SQLTask.run(getCurrentOdps(), sql).waitForSuccess();
	}

	@Override
	public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
		throws TableNotExistException, TableAlreadyExistException, CatalogException {

		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}

		ObjectPath newObjectPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);

		if (tableExists(newObjectPath)) {
			throw new TableAlreadyExistException(getName(), newObjectPath);
		}

		try {
			execute(
				String.format(
					"ALTER TABLE %s RENAME TO %s",
					tablePath.getFullName(),
					newObjectPath.getFullName()
				)
			);
		} catch (Exception e) {
			throw new CatalogException(e);
		}
	}

	private static String checkAndConvertPartitionSpecFormat(String partition) {
		if (StringUtils.isNullOrWhitespaceOnly(partition)) {
			return partition;
		} else {
			String odpsPartition = partition.replace("/", ",").replace(" ", "");
			try {
				return new PartitionSpec(odpsPartition).toString().replace("'", "");
			} catch (Exception e) {
				throw new IllegalArgumentException("Invalid partition format: \"" + partition + "\"");
			}
		}
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
		throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

		String partition = table.getProperties().get(OdpsSinkParams.PARTITION.getName());
		partition = checkAndConvertPartitionSpecFormat(partition);

		if (!StringUtils.isNullOrWhitespaceOnly(partition) && tableExists(tablePath)) {
			return;
		}

		com.aliyun.odps.TableSchema odpsSchema = new com.aliyun.odps.TableSchema();
		String[] colNames = table.getSchema().getFieldNames();
		TypeInformation <?>[] colTypes = table.getSchema().getFieldTypes();
		for (int i = 0; i < colNames.length; ++i) {
			odpsSchema.addColumn(OdpsTableUtil.createOdpsColumn(colNames[i], colTypes[i]));
		}

		if (null != partition && !partition.isEmpty()) {
			String[] strs = partition.split(",");
			for (String str : strs) {
				String partitionKey = str.split("=")[0].trim();
				odpsSchema.addPartitionColumn(new Column(partitionKey, OdpsType.STRING));
			}
		}

		String lifeCycleStr = table.getProperties().get(OdpsSinkParams.LIFE_CYCLE.getName());

		try {
			if (lifeCycleStr == null || Long.parseLong(lifeCycleStr) <= 0) {
				getCurrentOdps().tables().create(
					tablePath.getDatabaseName(),
					tablePath.getObjectName(),
					odpsSchema,
					null, false
				);
			} else {
				getCurrentOdps().tables().createTableWithLifeCycle(
					tablePath.getDatabaseName(),
					tablePath.getObjectName(),
					odpsSchema,
					null,
					false,
					Long.parseLong(lifeCycleStr)
				);
			}
		} catch (OdpsException e) {
			throw new CatalogException(e);
		}
	}

	@Override
	public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
		throws TableNotExistException, CatalogException {

		throw new UnsupportedOperationException();
	}

	private static PartitionSpec mapToPartitionSpec(Map <String, String> map) {
		StringBuilder stringBuilder = new StringBuilder();

		boolean in = false;

		for (Map.Entry <String, String> entry : map.entrySet()) {
			if (!in) {
				stringBuilder.append("/");
				in = true;
			}

			stringBuilder.append(entry.getKey()).append("=").append(entry.getValue());
		}

		return new PartitionSpec(stringBuilder.toString());
	}

	private static Map <String, String> partitionSpecToMap(PartitionSpec partitionSpec) {

		Map <String, String> partitionMap = new LinkedHashMap <>();

		for (String key : partitionSpec.keys()) {
			partitionMap.put(key, partitionSpec.get(key));
		}

		return partitionMap;
	}

	@Override
	public List <CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
		throws TableNotExistException, TableNotPartitionedException, CatalogException {
		try {
			List <Partition> partitions = getCurrentOdps()
				.tables().get(tablePath.getDatabaseName(), tablePath.getObjectName())
				.getPartitions();

			List <CatalogPartitionSpec> partitionSpecs = new ArrayList <>(partitions.size());

			for (Partition partition : partitions) {
				partitionSpecs.add(
					new CatalogPartitionSpec(
						partitionSpecToMap(partition.getPartitionSpec())
					)
				);
			}

			return partitionSpecs;

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public List <CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws TableNotExistException, TableNotPartitionedException, CatalogException {
		try {
			Iterator <Partition> partitionIterator = getCurrentOdps()
				.tables().get(tablePath.getDatabaseName(), tablePath.getObjectName())
				.getPartitionIterator(mapToPartitionSpec(partitionSpec.getPartitionSpec()));

			List <CatalogPartitionSpec> partitionSpecs = new ArrayList <>();

			while (partitionIterator.hasNext()) {
				Partition partition = partitionIterator.next();

				PartitionSpec partitionSpecIn = partition.getPartitionSpec();

				Map <String, String> partitionMap = new LinkedHashMap <>();

				for (String key : partitionSpecIn.keys()) {
					partitionMap.put(key, partitionSpecIn.get(key));
				}

				partitionSpecs.add(new CatalogPartitionSpec(partitionMap));
			}

			return partitionSpecs;

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
		throws PartitionNotExistException, CatalogException {
		try {
			Partition partition = getCurrentOdps()
				.tables().get(tablePath.getDatabaseName(), tablePath.getObjectName())
				.getPartition(mapToPartitionSpec(partitionSpec.getPartitionSpec()));

			return new CatalogPartitionImpl(partitionSpecToMap(partition.getPartitionSpec()), "");

		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
		try {
			return getCurrentOdps()
				.tables().get(tablePath.getDatabaseName(), tablePath.getObjectName())
				.hasPartition(mapToPartitionSpec(partitionSpec.getPartitionSpec()));
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition,
								boolean ignoreIfExists)
		throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
		PartitionAlreadyExistsException, CatalogException {

		try {
			getCurrentOdps()
				.tables().get(tablePath.getDatabaseName(), tablePath.getObjectName())
				.createPartition(mapToPartitionSpec(partitionSpec.getPartitionSpec()), ignoreIfExists);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
		throws PartitionNotExistException, CatalogException {

		try {
			getCurrentOdps()
				.tables().get(tablePath.getDatabaseName(), tablePath.getObjectName())
				.deletePartition(mapToPartitionSpec(partitionSpec.getPartitionSpec()), ignoreIfNotExists);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition,
							   boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List <String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
		List <String> functions = new ArrayList <>();

		getCurrentOdps()
			.functions()
			.iterable(dbName)
			.forEach(function -> functions.add(function.getName()));

		return functions;
	}

	@Override
	public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
		Function odpsFunction = getCurrentOdps()
			.functions()
			.get(functionPath.getDatabaseName(), functionPath.getObjectName());

		return new CatalogFunctionImpl(odpsFunction.getClassPath(), new HashMap <>());
	}

	@Override
	public boolean functionExists(ObjectPath functionPath) throws CatalogException {
		try {
			return getCurrentOdps()
				.functions()
				.exists(functionPath.getDatabaseName(), functionPath.getObjectName());
		} catch (OdpsException e) {
			throw new CatalogException(e);
		}
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

		if (!functionExists(functionPath)) {
			throw new FunctionNotExistException(getName(), functionPath);
		}

		try {
			getCurrentOdps()
				.functions()
				.delete(functionPath.getDatabaseName(), functionPath.getObjectName());
		} catch (OdpsException e) {
			throw new CatalogException(e);
		}
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

	private static String[] getReadingPartitions(
		String tableName, List <PartitionSpec> allPartitionSpecs, Params params) {

		String key = "partitions";
		String[] selectedPartitionSpecs = null;
		String[] allPartitions;

		allPartitions = new String[allPartitionSpecs.size()];
		for (int i = 0; i < allPartitions.length; i++) {
			allPartitions[i] = allPartitionSpecs.get(i).toString().replace("'", "");
		}

		if (!params.contains(key)) {
			return allPartitions;
		}

		String str = params.get(OdpsSourceParams.PARTITIONS);
		if (StringUtils.isNullOrWhitespaceOnly(str)) {
			return allPartitions;
		}
		selectedPartitionSpecs = str.split(",");

		for (int i = 0; i < selectedPartitionSpecs.length; i++) {
			selectedPartitionSpecs[i] = checkAndConvertPartitionSpecFormat(selectedPartitionSpecs[i]);
		}

		Set <PartitionSpec> qualifiedPartitionSpecs = new HashSet <>();

		for (String selectedPartitionSpec : selectedPartitionSpecs) {
			PartitionSpec spec;
			try {
				spec = new PartitionSpec(selectedPartitionSpec);
			} catch (Exception e) {
				throw new IllegalArgumentException("Invalid partition format: " + selectedPartitionSpec);
			}
			Set <String> partColNames = spec.keys();

			// Loop over all table partitions to pick the qualified ones
			// Note that we allow partial specifications (i.e., specifying "ds=2018" only, while the table has two
			// partition
			// columns "ds" and "dt".
			int cnt = 0;
			for (int j = 0; j < allPartitionSpecs.size(); j++) {
				PartitionSpec target = allPartitionSpecs.get(j);
				if (j == 0) {
					// check col names
					for (String colName : partColNames) {
						if (!target.keys().contains(colName)) {
							throw new IllegalArgumentException(String.format(
								"Invalid partition spec \"%s\", col \"%s\" is not partition columns of input table "
									+ "\"%s\".",
								selectedPartitionSpec, colName, tableName));
						}
					}
				}
				boolean bingo = true;
				for (String colName : partColNames) {
					if (!spec.get(colName).equals(target.get(colName))) {
						bingo = false;
						break;
					}
				}
				if (bingo) {
					qualifiedPartitionSpecs.add(target);
					cnt++;
				}
			}

			if (cnt == 0) {
				throw new IllegalArgumentException(String.format(
					"Invalid partition spec \"%s\", no corresponding partitions are found in input table \"%s\".",
					selectedPartitionSpec, tableName));
			}
		}

		String[] qualifiedPartitions = new String[qualifiedPartitionSpecs.size()];
		int pos = 0;
		for (PartitionSpec spec : qualifiedPartitionSpecs) {
			qualifiedPartitions[pos++] = spec.toString().replace("'", "");
		}

		return qualifiedPartitions;
	}

	public List <PartitionSpec> listPartitionSpecs(ObjectPath objectPath) {
		try {
			List <Partition> partitions = getCurrentOdps()
				.tables()
				.get(objectPath.getDatabaseName(), objectPath.getObjectName())
				.getPartitions();

			List <PartitionSpec> partitionSpecs = new ArrayList <>(partitions.size());
			for (Partition par : partitions) {
				partitionSpecs.add(par.getPartitionSpec());
			}
			return partitionSpecs;
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	protected RichInputFormat <Row, InputSplit> createInputFormat(
		ObjectPath objectPath, TableSchema schema, Params params) throws Exception {

		OdpsConf conf = new OdpsConf(
			params.get(OdpsCatalogParams.ACCESS_ID),
			params.get(OdpsCatalogParams.ACCESS_KEY),
			params.get(OdpsCatalogParams.END_POINT),
			params.get(OdpsCatalogParams.PROJECT)
		);

		boolean isPartitioned = isPartitioned(objectPath);
		String[] partitions = null;

		if (isPartitioned) {
			partitions = getReadingPartitions(
				objectPath.getFullName(),
				listPartitionSpecs(objectPath),
				params
			);
		}

		int parallelism = params.get(PARALLELISM);

		return new OdpsInputFormat(
			objectPath.getObjectName(),
			partitions,
			parallelism,
			conf,
			getTable(objectPath).getSchema().getFieldNames()
		);
	}

	public boolean isPartitioned(ObjectPath objectPath) throws OdpsException {
		return getCurrentOdps()
			.tables()
			.get(objectPath.getDatabaseName(), objectPath.getObjectName())
			.isPartitioned();
	}

	private void checkPartitionSpecForSinking(ObjectPath objectPath, PartitionSpec partitionSpec) {

		List <Column> partitionColsInTable = getCurrentOdps()
			.tables()
			.get(objectPath.getDatabaseName(), objectPath.getObjectName())
			.getSchema()
			.getPartitionColumns();

		Set <String> partitionsColNamesInSpec = partitionSpec.keys();

		// check
		for (String colName : partitionsColNamesInSpec) {
			boolean found = false;
			for (Column col : partitionColsInTable) {
				if (colName.equals(col.getName())) {
					found = true;
					break;
				}
			}
			if (!found) {
				throw new RuntimeException(
					String.format("Invalid partition spec \"%s\", \"%s\" is not partition col in output table \"%s\"",
						partitionSpec.toString(), colName, objectPath.getObjectName()));
			}
		}
		for (Column col : partitionColsInTable) {
			String colNameInTable = col.getName();
			boolean found = partitionsColNamesInSpec.contains(colNameInTable);
			if (!found) {
				throw new RuntimeException(
					String.format("Invalid partition spec \"%s\", missing partition col \"%s\" of output table \"%s\"",
						partitionSpec.toString(), colNameInTable, objectPath.getObjectName()));
			}
		}
	}

	private static boolean isSameType(TypeInformation <?> flinkType, OdpsType odpsType) {
		if (flinkType.equals(Types.STRING())) {
			return odpsType.equals(OdpsType.STRING);
		} else if (flinkType.equals(Types.BOOLEAN())) {
			return odpsType.equals(OdpsType.BOOLEAN);
		} else if (flinkType.equals(Types.BYTE())) {
			return false;
		} else if (flinkType.equals(Types.SHORT())) {
			return odpsType.equals(OdpsType.SMALLINT);
		} else if (flinkType.equals(Types.INT())) {
			return odpsType.equals(OdpsType.INT);
		} else if (flinkType.equals(Types.LONG())) {
			return odpsType.equals(OdpsType.BIGINT);
		} else if (flinkType.equals(Types.FLOAT())) {
			return odpsType.equals(OdpsType.FLOAT);
		} else if (flinkType.equals(Types.DOUBLE())) {
			return odpsType.equals(OdpsType.DOUBLE);
		} else if (flinkType.equals(Types.DECIMAL())) {
			return odpsType.equals(OdpsType.DECIMAL);
		} else if (flinkType.equals(Types.SQL_DATE())) {
			return odpsType.equals(OdpsType.DATE);
		} else if (flinkType.equals(Types.SQL_TIME())) {
			return odpsType.equals(OdpsType.DATETIME);
		} else if (flinkType.equals(Types.SQL_TIMESTAMP())) {
			return odpsType.equals(OdpsType.DATETIME) || odpsType.equals(OdpsType.TIMESTAMP);
		} else {
			return false;
		}
	}

	private void checkSchema(ObjectPath objectPath, TableSchema schema) {
		List <Column> cols = getCurrentOdps()
			.tables()
			.get(objectPath.getDatabaseName(), objectPath.getObjectName())
			.getSchema()
			.getColumns();

		String[] colNames = schema.getFieldNames();
		TypeInformation <?>[] colTypes = schema.getFieldTypes();

		if (cols.size() != colNames.length) {
			throw new RuntimeException(
				String.format("mismatched schema size between outputting operator and the overwritten table \"%s\"." +
					"Outputting table schema is %s", objectPath.getFullName(), schema.toString()));
		}

		// loop over odps table cols:
		for (int i = 0; i < cols.size(); i++) {
			Column col = cols.get(i);
			String name = col.getName();
			int idx = TableUtil.findColIndex(colNames, name);
			if (idx < 0) {
				throw new RuntimeException(String.format(
					"odps table col \"%s\" of table \"%s\" can't be found in the outputting operator schema: %s",
					name, objectPath.getFullName(), schema.toString()));
			}
			if (!isSameType(colTypes[idx], col.getType())) {
				throw new RuntimeException(String.format(
					"mismatched col type between col \"%s\" of table \"%s\" and the outputting operator: %s vs %s",
					name, objectPath.getFullName(), colTypes[i].toString(), col.getType().toString()));
			}
		}
	}

	@Override
	protected OutputFormat <Row> createOutputFormat(ObjectPath objectPath, TableSchema schema, Params params) {
		boolean isOverwriteSink = params.get(OdpsSinkParams.OVERWRITE_SINK);

		String partition = params.get(OdpsSinkParams.PARTITION);
		partition = checkAndConvertPartitionSpecFormat(partition);
		if (StringUtils.isNullOrWhitespaceOnly(partition)) {
			partition = "";
		}

		//Create Table
		try {
			if (tableExists(objectPath)) {
				if (isPartitioned(objectPath)) {

					if (StringUtils.isNullOrWhitespaceOnly(partition)) {
						throw new IllegalArgumentException(String.format(
							"partition spec is empty while outputting to a partition table \"%s\"",
							objectPath.getObjectName()));
					}

					PartitionSpec partitionSpec = new PartitionSpec(partition);

					checkPartitionSpecForSinking(objectPath, partitionSpec);

					if (getCurrentOdps()
						.tables()
						.get(objectPath.getDatabaseName(), objectPath.getObjectName())
						.hasPartition(partitionSpec)) {

						if (!isOverwriteSink) {
							throw new RuntimeException(String.format(
								"the specified partition \"%s\" already exists in output table \"%s\"",
								partition, objectPath.getObjectName()));
						}
					}

					checkSchema(objectPath, schema);
				} else {
					// for non-partitioned table
					if (!isOverwriteSink) {
						throw new RuntimeException("table already exists: " + objectPath.getFullName());
					} else {
						if (!StringUtils.isNullOrWhitespaceOnly(partition)) {
							throw new IllegalArgumentException(String.format(
								"partition spec \"%s\" is not empty while outputting to non-partition table \"%s\"",
								partition, objectPath.getFullName()));
						}
						checkSchema(objectPath, schema);
					}
				}
			} else {
				createTable(objectPath, new CatalogTableImpl(schema, new HashMap <>(), ""), false);
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		OdpsConf conf = new OdpsConf(
			params.get(OdpsCatalogParams.ACCESS_ID),
			params.get(OdpsCatalogParams.ACCESS_KEY),
			params.get(OdpsCatalogParams.END_POINT),
			params.get(OdpsCatalogParams.PROJECT)
		);

		return new OdpsOutputFormat(conf, objectPath.getObjectName(), partition, isOverwriteSink);
	}

	public static class OdpsInputFormat extends RichInputFormat <Row, InputSplit> {
		private final int numPartitions;
		private final OdpsConf odpsConf;
		private final String table;
		private final String[] partitions;
		private final String[] fieldNames;

		private transient OdpsTableSchema odpsTableSchema;
		private transient OdpsType[] fieldOdpsTypes;
		private transient TableTunnel tableTunnel;
		private transient Odps odps;

		private Tuple2 <String[], long[]> downloadSessionsAndCounts;
		private String[] selectedColumns;
		private NextIterator <Record> iter;

		public OdpsInputFormat(
			String table, String[] partitions, int numPartitions, OdpsConf odpsConf, String[] fieldNames) {

			this.fieldNames = fieldNames;
			this.odpsConf = odpsConf;
			this.table = table;
			this.partitions = partitions == null ? new String[0] : partitions;
			this.numPartitions = numPartitions;
		}

		@Override
		public void configure(Configuration parameters) {

		}

		@Override
		public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
			return null;
		}

		private Odps getOdps() {
			this.odps = OdpsUtils.initOdps(this.odpsConf);
			return this.odps;
		}

		private void checkPartitions(Odps odps, String table) throws IOException {
			Table t = odps.tables().get(table);

			for (String partition : this.partitions) {
				PartitionSpec partitionSpec = new PartitionSpec(partition);

				try {
					if (!t.hasPartition(partitionSpec)) {
						throw new IOException("partition not exist. " + partitionSpec.toString());
					}
				} catch (OdpsException var10) {
					throw new IOException("check partition failed!", var10);
				}
			}

		}

		@Override
		public void openInputFormat() {
			this.odps = this.getOdps();
			if (this.odpsTableSchema == null) {
				try {
					this.checkPartitions(this.odps, this.table);
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}

				try {
					this.odpsTableSchema = OdpsUtils.getODPSTableSchema(this.odps, this.table);
				} catch (IOException var4) {
					throw new RuntimeException("get table schema failed!", var4);
				}
			}

			this.tableTunnel = new TableTunnel(this.odps);
			if (!this.odpsConf.getTunnelEndpoint().isEmpty()) {
				this.tableTunnel.setEndpoint(this.odpsConf.getTunnelEndpoint());
			}

			if (this.fieldNames.length > 0) {
				this.selectedColumns = this.fieldNames;
			} else {
				this.selectedColumns = new String[this.odpsTableSchema.getColumns().size()];
				int i = 0;

				OdpsColumn odpsColumn;

				for (Iterator <OdpsColumn> iter = this.odpsTableSchema.getColumns().iterator();
					 iter.hasNext(); this.selectedColumns[i++] = odpsColumn.getName()) {

					odpsColumn = iter.next();
				}
			}
		}

		private void setDownloadSessionAndCountIfAbsent() throws IOException {
			if (downloadSessionsAndCounts == null) {
				Odps odps = OdpsUtils.initOdps(odpsConf);
				TableTunnel tunnel = new TableTunnel(odps);
				if (!odpsConf.getTunnelEndpoint().isEmpty()) {
					tunnel.setEndpoint(odpsConf.getTunnelEndpoint());
				}

				if (odpsTableSchema == null) {
					odpsTableSchema = OdpsUtils.getODPSTableSchema(odps, table);
				}

				int len = odpsTableSchema.isPartition() ? partitions.length : 1;
				String[] sessionIds = new String[len];
				long[] counts = new long[len];
				if (!odpsTableSchema.isPartition()) {
					DownloadSession downloadSession = OdpsMetadataProvider.createDownloadSession(tunnel,
						odpsConf.getProject(), table);
					sessionIds[0] = downloadSession.getId();
					counts[0] = downloadSession.getRecordCount();
				} else {
					if (len == 0) {
						throw new IOException(
							"[" + table + "] is a partitioned table, but no partitions specified!");
					}

					for (int i = 0; i < len; ++i) {
						DownloadSession downloadSession = OdpsMetadataProvider.createDownloadSession(tunnel,
							odpsConf.getProject(), table, new PartitionSpec(partitions[i]));
						sessionIds[i] = downloadSession.getId();
						counts[i] = downloadSession.getRecordCount();
					}
				}

				downloadSessionsAndCounts = Tuple2.of(sessionIds, counts);
			}
		}

		@Override
		public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
			Preconditions.checkArgument(numPartitions >= 1, "splits num cannot be less than 1!");
			setDownloadSessionAndCountIfAbsent();
			long[] counts = downloadSessionsAndCounts.f1;
			ElementSegment[][] splitsResult = ElementSplitUtil.doSplit(counts, numPartitions);

			String[] sessionIds = downloadSessionsAndCounts.f0;
			OdpsInputSplit[] splits = new OdpsInputSplit[numPartitions];

			for (int splitIdx = 0; splitIdx < numPartitions; ++splitIdx) {
				ElementSegment[] segmentsInOneSplit = splitsResult[splitIdx];
				List <OdpsPartitionSegmentDownloadDesc> partitionSegments =
					new ArrayList <>(segmentsInOneSplit.length);

				for (ElementSegment segment : segmentsInOneSplit) {
					if (segment.getCount() > 0L) {
						int partitionId = segment.getElementId();
						String partition = odpsTableSchema.isPartition() ? partitions[partitionId] : null;
						String sessionId = sessionIds[partitionId];
						partitionSegments.add(
							new OdpsPartitionSegmentDownloadDesc(partition, segment.getStart(), segment.getCount(),
								sessionId));
					}
				}

				OdpsPartitionSegmentDownloadDesc[] container
					= new OdpsPartitionSegmentDownloadDesc[partitionSegments.size()];
				splits[splitIdx] = new OdpsInputSplit(splitIdx, partitionSegments.toArray(container));
			}

			return splits;
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
			return new DefaultInputSplitAssigner(inputSplits);
		}

		abstract static class NextIterator<T> implements Iterator <T>, Closeable {
		}

		class LocalIter extends NextIterator <Record> {
			OdpsInputSplitReader reader;
			Tuple2 <Record, PartitionSpec> content;
			Column[] fullColumns;
			Record fullRecord;

			LocalIter(OdpsInputSplitReader reader) throws TunnelException {
				this.reader = reader;

				fullColumns = new Column[selectedColumns.length];

				for (int i = 0; i < fullColumns.length; ++i) {
					fullColumns[i] = new Column(
						odpsTableSchema.getColumn(selectedColumns[i]).getName(),
						odpsTableSchema.getColumn(selectedColumns[i]).getType()
					);
				}

				fullRecord = new ArrayRecord(fullColumns);
			}

			public void close() {
				reader.close();
			}

			public void remove() {
				throw new UnsupportedOperationException("remove");
			}

			public boolean hasNext() {
				if (!reader.hasNext()) {
					return false;
				} else {
					try {
						content = reader.next();
					} catch (Exception ex) {
						throw new RuntimeException("read record failed!", ex);
					}

					if (content == null) {
						IOUtils.closeQuietly(reader);
					}

					return content != null;
				}
			}

			public Record next() {
				if (content == null) {
					try {
						content = reader.next();
					} catch (Exception ex) {
						throw new RuntimeException("read record failed!", ex);
					}
				}

				Record record = content.f0;
				PartitionSpec part = content.f1;
				content = null;
				if (part != null) {
					for (String selectedColumn : selectedColumns) {
						OdpsColumn column = odpsTableSchema.getColumn(selectedColumn);
						if (column.isPartition()) {
							fullRecord.set(column.getName(),
								column.getType().equals(OdpsType.STRING) ? part.get(column.getName())
									: Long.valueOf(part.get(column.getName())));
						} else {
							fullRecord.set(column.getName(), record.get(column.getName()));
						}
					}

					return fullRecord;
				} else {
					return record;
				}
			}
		}

		@Override
		public void open(InputSplit split) throws IOException {
			int numFields = selectedColumns.length;
			int length = selectedColumns.length;

			Set <String> columnsSet = new HashSet <>(Arrays.asList(selectedColumns).subList(0, length));

			fieldOdpsTypes = new OdpsType[numFields];
			int fieldPos = 0;
			Iterator <OdpsColumn> odpsColumnIter = odpsTableSchema.getColumns().iterator();

			while (true) {
				OdpsColumn column;
				do {
					if (!odpsColumnIter.hasNext()) {
						OdpsInputSplitReader reader = new OdpsInputSplitReader(
							(OdpsInputSplit) split,
							selectedColumns,
							odpsTableSchema,
							tableTunnel,
							odpsConf.getProject(),
							table
						);

						try {
							iter = new LocalIter(reader);
						} catch (TunnelException ex) {
							throw new IOException(ex);
						}

						return;
					}

					column = odpsColumnIter.next();
				} while (columnsSet.size() > 0 && !columnsSet.contains(column.getName()));

				fieldOdpsTypes[fieldPos] = column.getType();
				++fieldPos;
			}
		}

		@Override
		public boolean reachedEnd() {
			return !iter.hasNext();
		}

		private Row buildFlinkRow(Row row, Record record) throws Exception {
			for (int i = 0; i < fieldNames.length; ++i) {
				Object o = record.get(fieldNames[i]);
				if (o == null) {
					row.setField(i, o);
				} else {
					switch (fieldOdpsTypes[i]) {
						case INT:
							if (o instanceof String) {
								row.setField(i, Integer.parseInt((String) o));
							} else {
								row.setField(i, o);
							}
							break;
						case TINYINT:
							if (o instanceof String) {
								row.setField(i, Byte.parseByte((String) o));
							} else {
								row.setField(i, o);
							}
							break;
						case SMALLINT:
							if (o instanceof String) {
								row.setField(i, Short.parseShort((String) o));
							} else {
								row.setField(i, o);
							}
							break;
						case BIGINT:
							if (o instanceof String) {
								row.setField(i, Long.parseLong((String) o));
							} else {
								row.setField(i, o);
							}
							break;
						case BOOLEAN:
							if (o instanceof String) {
								row.setField(i, Boolean.parseBoolean((String) o));
							} else {
								row.setField(i, o);
							}
							break;
						case FLOAT:
							if (o instanceof String) {
								row.setField(i, Float.parseFloat((String) o));
							} else {
								row.setField(i, o);
							}
							break;
						case DOUBLE:
							if (o instanceof String) {
								row.setField(i, Double.parseDouble((String) o));
							} else {
								row.setField(i, o);
							}
							break;
						case STRING:
							if (o instanceof String) {
								row.setField(i, o);
							} else {
								row.setField(i, new String((byte[]) o, StandardCharsets.UTF_8));
							}
							break;
						case DATETIME:
							if (o instanceof String) {
								row.setField(i, DateFormat.getTimeInstance().parse((String) o));
							} else {
								row.setField(i, o);
							}
							break;
						case DECIMAL:
							if (o instanceof String) {
								row.setField(i, new BigDecimal((String) o));
							} else {
								row.setField(i, o);
							}
						case BINARY:
							if (o instanceof String) {
								throw new RuntimeException("Cannot handle partition keys of type BINARY.");
							}

							row.setField(i, o);
							break;
						case ARRAY:
							if (o instanceof String) {
								throw new RuntimeException("Cannot handle partition keys of type ARRAY.");
							}

							row.setField(i, o);
							break;
						case MAP:
							if (o instanceof String) {
								throw new RuntimeException("Cannot handle partition keys of type MAP.");
							}

							row.setField(i, o);
							break;
						case STRUCT:
							if (o instanceof String) {
								throw new RuntimeException("Cannot handle partition keys of type STRUCT.");
							}

							row.setField(i, o);
							break;
						default:
							throw new RuntimeException("Invalid Type");
					}
				}
			}

			return row;
		}

		@Override
		public Row nextRecord(Row reuse) throws IOException {
			try {
				return buildFlinkRow(reuse, iter.next());
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		@Override
		public void close() throws IOException {
			if (iter != null) {
				iter.close();
			}
		}
	}

	public static class OdpsOutputFormat extends RichOutputFormat <Row>
		implements InitializeOnMaster, FinalizeOnMaster {

		private static final long serialVersionUID = 1L;

		public static final String DEFAULT_LABEL = "__default__";

		private final Map <String, String> label2PartitionMap;
		private final String tableName;
		private final boolean isOverwrite;
		private final OdpsConf odpsConf;

		private transient Record odpsRecord;
		private transient Map <String, UploadSession> label2UploadSessionMap;
		private transient Map <String, RecordWriter> label2RecordWriterMap;
		private transient Odps odps;

		public OdpsOutputFormat(OdpsConf odpsConf, String tableName, String partition, boolean isOverwrite) {
			this.odpsConf = odpsConf;
			this.tableName = tableName;
			this.isOverwrite = isOverwrite;
			this.label2PartitionMap = new HashMap <>();

			this.label2PartitionMap.put(DEFAULT_LABEL, partition);
		}

		@Override
		public void finalizeGlobal(int parallelism) {
			// pass
		}

		public String buildOdpsRecord(Record odpsRecord, Row record) {
			assert record.getArity() == odpsRecord.getColumnCount();

			for (int i = 0; i < record.getArity(); ++i) {
				odpsRecord.set(i, record.getField(i));
			}

			return DEFAULT_LABEL;
		}

		@Override
		public void initializeGlobal(int parallelism) {
			this.odps = this.getOdps();

			boolean isPartitioned;
			try {
				isPartitioned = this.odps.tables().get(this.odpsConf.project, this.tableName).isPartitioned();
			} catch (OdpsException ex) {
				throw new RuntimeException("get table failed" + ex);
			}

			if (!isPartitioned) {
				assert this.label2PartitionMap.size() == 1;

				if (!this.label2PartitionMap.containsKey(DEFAULT_LABEL)) {
					throw new RuntimeException("Non-partition table should not change output label.");
				}
			}

			if (!isPartitioned) {
				OdpsUtils.dealTruncate(this.getOdps(), this.odpsConf.project, this.tableName, "", this.isOverwrite);
			} else {

				for (Entry <String, String> stringStringEntry : this.label2PartitionMap.entrySet()) {
					OdpsUtils.dealTruncate(this.getOdps(), this.odpsConf.project, this.tableName,
						stringStringEntry.getValue(),
						this.isOverwrite);
				}
			}
		}

		@Override
		public void configure(Configuration parameters) {
			// pass
		}

		private Odps getOdps() {
			this.odps = OdpsUtils.initOdps(this.odpsConf);
			return this.odps;
		}

		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
			com.aliyun.odps.TableSchema tableSchema = OdpsUtils.getTableSchema(
				this.getOdps(), this.odpsConf.project, this.tableName
			);

			this.odpsRecord = new ArrayRecord(tableSchema);

			this.label2UploadSessionMap = new HashMap <>();
			this.label2RecordWriterMap = new HashMap <>();

			TableTunnel tunnel = new TableTunnel(this.getOdps());
			if (!this.odpsConf.getTunnelEndpoint().isEmpty()) {
				tunnel.setEndpoint(this.odpsConf.getTunnelEndpoint());
			}

			for (Entry <String, String> stringStringEntry : this.label2PartitionMap.entrySet()) {
				UploadSession uploadSession;
				RecordWriter recordWriter;
				if (com.aliyun.odps.utils.StringUtils.isEmpty(stringStringEntry.getValue())) {
					try {
						uploadSession = tunnel.createUploadSession(this.odpsConf.project, this.tableName);
						recordWriter = uploadSession.openBufferedWriter();
						this.label2UploadSessionMap.put(stringStringEntry.getKey(), uploadSession);
						this.label2RecordWriterMap.put(stringStringEntry.getKey(), recordWriter);
					} catch (TunnelException ex) {
						throw new IOException(ex);
					}
				} else {
					try {
						uploadSession = tunnel.createUploadSession(this.odpsConf.project, this.tableName,
							new PartitionSpec(stringStringEntry.getValue()));
						recordWriter = uploadSession.openBufferedWriter();
						this.label2UploadSessionMap.put(stringStringEntry.getKey(), uploadSession);
						this.label2RecordWriterMap.put(stringStringEntry.getKey(), recordWriter);
					} catch (TunnelException ex) {
						throw new IOException(ex);
					}
				}
			}
		}

		@Override
		public void writeRecord(Row record) throws IOException {
			for (int i = 0; i < this.odpsRecord.getColumnCount(); ++i) {
				this.odpsRecord.set(i, null);
			}

			String label = this.buildOdpsRecord(this.odpsRecord, record);

			if (!this.label2RecordWriterMap.containsKey(label)) {
				throw new IOException("Cannot find label: " + label + " in output table configs!");
			}

			RecordWriter recordWriter = this.label2RecordWriterMap.get(label);
			recordWriter.write(this.odpsRecord);
		}

		@Override
		public void close() throws IOException {

			for (Entry <String, RecordWriter> entry : this.label2RecordWriterMap.entrySet()) {
				entry.getValue().close();
			}

			for (Entry <String, UploadSession> entry : this.label2UploadSessionMap.entrySet()) {
				try {
					entry.getValue().commit();
				} catch (TunnelException ex) {
					throw new IOException(ex);
				}
			}
		}
	}
}
