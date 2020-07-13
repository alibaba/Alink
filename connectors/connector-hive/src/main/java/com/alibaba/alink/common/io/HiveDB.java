package com.alibaba.alink.common.io;

import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.DBAnnotation;
import com.alibaba.alink.common.io.table.BaseDbTable;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.HiveDBParams;
import com.alibaba.alink.params.io.HiveSinkParams;
import com.alibaba.alink.params.io.HiveSourceParams;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connectors.hive.*;
import org.apache.flink.connectors.hive.write.HiveOutputFormatFactory;
import org.apache.flink.connectors.hive.write.HiveWriterFactory;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.factories.TableSinkFactoryContextImpl;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.filesystem.FileSystemOutputFormat;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.*;

import static org.apache.flink.table.filesystem.FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_KIND;

/**
 * Some pitfalls:
 * 1) datanucleus-api-jdo-4.2.4.jar and datanucleus-core-4.1.17.jar plugin.xml conflict
 * 2) flink config should: classloader.resolve-order: parent-first
 * 3) Hive client version should match with the target Hive to the level of middle version number (like 2.3.0)
 * Otherwise, the target Hive's schema version may be corrupted by the hive client (changed to 2.0.0 for example)
 */

@DBAnnotation(name = "hive")
public class HiveDB extends BaseDB implements Serializable {
    private HiveCatalog catalog;
    private String dbName;
    private String hiveVersion;
    private String hiveConfDir;

    private static final Logger LOG = LoggerFactory.getLogger(HiveDB.class);

    public HiveDB(String hiveConfDir, String hiveVersion, String dbName) {
        this(new Params().set(HiveDBParams.HIVE_CONF_DIR, hiveConfDir)
            .set(HiveDBParams.HIVE_VERSION, hiveVersion)
            .set(HiveDBParams.DB_NAME, dbName));
    }

    public HiveDB(Params params) {
        super(params);
        checkHiveVersion();
        dbName = params.get(HiveDBParams.DB_NAME);
        hiveConfDir = params.get(HiveDBParams.HIVE_CONF_DIR);
        hiveVersion = params.get(HiveDBParams.HIVE_VERSION);
    }

    private void checkHiveVersion() {
        String hiveVersion = params.get(HiveDBParams.HIVE_VERSION);
    }

    public String getHiveVersion() {
        return hiveVersion;
    }

    public String getHiveConfDir() {
        return hiveConfDir;
    }

    private HiveCatalog getCatalog() throws Exception {
        if (catalog == null) {
            String hiveConfDir = params.get(HiveDBParams.HIVE_CONF_DIR);
            Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(hiveConfDir));
            String hiveVersion = params.get(HiveDBParams.HIVE_VERSION);
            String catalogName = FileUtils.getRandomFilename("");
            if (!hiveConfDir.startsWith("/")) { // remove conf dir
                String tempDir = System.getProperty("java.io.tmpdir");
                File localConfDir = new File(tempDir, FileUtils.getRandomFilename(""));
                FileUtils.copy(new Path(hiveConfDir + "/hive-site.xml"),
                    new Path(localConfDir.getAbsolutePath() + "/hive-site.xml"), false);
                hiveConfDir = localConfDir.getAbsolutePath();
                catalog = new HiveCatalog(catalogName, dbName, hiveConfDir, hiveVersion);
                catalog.open();
                LOG.info("open hive catalog success.");
            } else {
                catalog = new HiveCatalog(catalogName, dbName, hiveConfDir, hiveVersion);
                catalog.open();
            }
        }
        return catalog;
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public List<String> listTableNames() throws Exception {
        return getCatalog().listTables(dbName);
    }

    @Override
    public void execute(String sql) throws Exception {
        throw new UnsupportedOperationException("not supported.");
    }

    private static Map<String, String> getStaticPartitionSpec(String partitionSpec) {
        Map<String, String> spec = new HashMap<>();
        if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
            String[] partitions = partitionSpec.split("/");
            for (int i = 0; i < partitions.length; i++) {
                String p = partitions[i];
                int pos = p.indexOf('=');
                Preconditions.checkArgument(pos > 0);
                String col = p.substring(0, pos);
                String val = p.substring(pos + 1);
                spec.put(col, val);
            }
        }
        return spec;
    }

    private static String[] getPartitionColumnsFromSpec(String partitionSpec) {
        String[] partitions = partitionSpec.split("/");
        for (int i = 0; i < partitions.length; i++) {
            String p = partitions[i];
            int pos = p.indexOf('=');
            Preconditions.checkArgument(pos > 0);
            String col = p.substring(0, pos);
            partitions[i] = col + " STRING";
        }
        return partitions;
    }

    @Override
    public void createTable(String tableName, TableSchema schema, Params parameter) throws Exception {
        String[] partitionCols = new String[0];
        String partitionSpec = parameter.get(HiveSinkParams.PARTITION);
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
            TypeInformation<?>[] fieldTypes = new TypeInformation[partitionCols.length];
            Arrays.fill(fieldTypes, Types.STRING);
            fieldTypes = ArrayUtils.addAll(schema.getFieldTypes(), fieldTypes);
            schema = new TableSchema(fieldNames, fieldTypes);
        }
        ObjectPath objectPath = ObjectPath.fromString(dbName + "." + tableName);
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogConfig.IS_GENERIC, "false");
        CatalogTable catalogTable = new CatalogTableImpl(schema, Arrays.asList(partitionCols), properties, tableName);
        getCatalog().createTable(objectPath, catalogTable, false);
        System.out.println("Hive table " + tableName + " created.");
        LOG.info("Hive table {} created", tableName);
    }

    @Override
    public void dropTable(String tableName) throws Exception {
        ObjectPath objectPath = ObjectPath.fromString(dbName + "." + tableName);
        getCatalog().dropTable(objectPath, true);
    }

    @Override
    public boolean hasTable(String table) throws Exception {
        List<String> tableNames = this.listTableNames();
        for (String name : tableNames) {
            if (table.equals(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean hasColumn(String table, String column) throws Exception {
        return TableUtil.findColIndex(getTableSchema(table), column) >= 0;
    }

    @Override
    public String[] getColNames(String tableName) throws Exception {
        return getTableSchema(tableName).getFieldNames();
    }

    private TableSchema getTableSchemaWithPartitionColumns(String tableName) throws Exception {
        ObjectPath op = ObjectPath.fromString(String.format("%s.%s", dbName, tableName));
        CatalogBaseTable tbl = getCatalog().getTable(op);
        TableSchema schema = tbl.getSchema();
        return schema;
    }

    @Override
    public TableSchema getTableSchema(String tableName) throws Exception {
        TableSchema schema = getTableSchemaWithPartitionColumns(tableName);

        // remove static partition columns
        List<String> staticPartCols = getPartitionCols(tableName);
        int numPartCols = staticPartCols.size();
        if (numPartCols > 0) {
            Set<String> partColsSet = new HashSet<>();
            partColsSet.addAll(staticPartCols);
            int n = 0;
            String[] fieldNames = new String[schema.getFieldNames().length - numPartCols];
            TypeInformation[] fieldTypes = new TypeInformation[fieldNames.length];
            TypeInformation[] allFieldTypes = schema.getFieldTypes();
            for (int i = 0; i < allFieldTypes.length; i++) {
                String fieldName = schema.getFieldNames()[i];
                if (partColsSet.contains(fieldName)) {
                    continue;
                }
                fieldNames[n] = fieldName;
                fieldTypes[n] = allFieldTypes[i];
                n++;
            }
            return new TableSchema(fieldNames, fieldTypes);
        } else {
            return schema;
        }
    }

    @Override
    public void close() throws Exception {
        if (catalog != null) {
            catalog.close();
            catalog = null;
        }
    }

    @Override
    public BaseDbTable getDbTable(String tableName) throws Exception {
        throw new UnsupportedOperationException("not supported.");
    }

    private static List<Map<String, String>> getSelectedPartitions(String[] partitionSpecs) {
        List<Map<String, String>> selected = new ArrayList<>();
        for (String s : partitionSpecs) {
            Map<String, String> spec = getStaticPartitionSpec(s);
            selected.add(spec);
        }
        return selected;
    }

    private boolean isTableExists(String tableName) {
        try {
            return hasTable(tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CatalogTable getCatalogTable(String tableName) {
        try {
            return (CatalogTable) getCatalog().getTable(ObjectPath.fromString(dbName + "." + tableName));
        } catch (Exception e) {
            LOG.warn("Failed to do getCatalog() for table:{}", tableName, e);
            throw new RuntimeException("Fail to get getCatalog() table: " + tableName, e);
        }
    }

    public List<String> getPartitionCols(String tableName) {
        return getCatalogTable(tableName).getPartitionKeys();
    }

    private boolean isPartitionTable(String tableName) {
        return getPartitionCols(tableName).size() > 0;
    }

    private void checkSchemaMatch(TableSchema outputSchema, String tableName) {
        TableSchema tableSchema;
        try {
            tableSchema = getTableSchema(tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String[] outputFields = outputSchema.getFieldNames();
        String[] tableFields = tableSchema.getFieldNames();

        if (outputFields.length != tableFields.length) {
            throw new RuntimeException(
                String.format("mismatched schema size between outputting operator and the overwritten table \"%s\"." +
                    "Outputting operator schema is %s", tableName, outputSchema.toString()));
        }

        for (int i = 0; i < outputFields.length; i++) {
            if (outputFields[i].compareToIgnoreCase(tableFields[i]) != 0) {
                throw new RuntimeException(
                    String.format("mismatched schema between outputting operator and the overwritten table \"%s\"." +
                        "Outputting operator schema is %s", tableName, outputSchema.toString()));
            }
        }
    }

    private void checkTableExistenceBeforeSink(String tableName, Table in, Params parameter) {
        boolean hasTable = isTableExists(tableName);
        boolean overwriteSink = parameter.get(HiveSinkParams.OVERWRITE_SINK);
        String partitionSpec = parameter.get(HiveSinkParams.PARTITION);

        if (hasTable) {
            boolean isPartTable = isPartitionTable(tableName);
            if (overwriteSink) {
                checkSchemaMatch(in.getSchema(), tableName);
            } else {
                throw new RuntimeException(String.format(
                    "Table %s exists, you may setOverwriteSink(true) to overwrite this table.", tableName));
            }
        } else {
            try {
                createTable(tableName, in.getSchema(), parameter);
            } catch (Exception e) {
                LOG.warn("Failed to create table {}", tableName, e);
                throw new RuntimeException("Fail to create table " + tableName, e);
            }
        }

//        boolean hasDynamicPartitionCols = parameter.get(HiveSinkParams.HAS_DYN_PART_COLS);
//        if (!hasDynamicPartitionCols) {
//            List<String> partCols = getPartitionCols(tableName); // include both dynamic and static partition columns
//            Map<String, String> staticPartSpec = getStaticPartitionSpec(parameter.get(HiveSinkParams.PARTITION));
//            Preconditions.checkArgument(partCols.size() == staticPartSpec.size(),
//                "Static partition spec does not match that of table: " + tableName);
//            for (String c : partCols) {
//                Preconditions.checkArgument(staticPartSpec.containsKey(c),
//                    "Can't find column in static partition spec: " + c);
//            }
//        }
    }

    private HiveTableSource getHiveTableSource(String tableName, Params parameter, StreamTableEnvironment stenv) throws Exception {
        ObjectPath objectPath = ObjectPath.fromString(dbName + "." + tableName);
        CatalogTable catalogTable = getCatalogTable(tableName);
        HiveTableFactory factory = new HiveTableFactory(getCatalog().getHiveConf());

        TableSourceFactoryContextImpl context = new TableSourceFactoryContextImpl(
            ObjectIdentifier.of(getCatalog().getName(), dbName, tableName),
            catalogTable,
            stenv.getConfig().getConfiguration()
        );

        HiveTableSource hiveTableSource = (HiveTableSource) factory.createTableSource(context);
        String partitionSpecsStr = parameter.get(HiveSourceParams.PARTITIONS);
        if (!StringUtils.isNullOrWhitespaceOnly(partitionSpecsStr)) {
            String[] partitionSpecs = partitionSpecsStr.split(",");
            List<Map<String, String>> selectedPartitions = getSelectedPartitions(partitionSpecs);
            hiveTableSource = (HiveTableSource) hiveTableSource.applyPartitionPruning(selectedPartitions);
        }
        return hiveTableSource;
    }

    private HiveBatchSource getHiveBatchSource(String tableName, Params parameter) throws Exception {
        ObjectPath objectPath = ObjectPath.fromString(dbName + "." + tableName);
        CatalogTable catalogTable = getCatalogTable(tableName);
        HiveBatchSource hiveTableSource = new HiveBatchSource(new JobConf(catalog.getHiveConf()), objectPath, catalogTable);
        String partitionSpecsStr = parameter.get(HiveSourceParams.PARTITIONS);
        if (!StringUtils.isNullOrWhitespaceOnly(partitionSpecsStr)) {
            String[] partitionSpecs = partitionSpecsStr.trim().split(",");
            List<Map<String, String>> selectedPartitions = getSelectedPartitions(partitionSpecs);
            hiveTableSource = (HiveBatchSource) hiveTableSource.applyPartitionPruning(selectedPartitions);
        }
        return hiveTableSource;
    }

    @Override
    public Table getBatchTable(String tableName, Params parameter, Long sessionId) throws Exception {
        ExecutionEnvironment env = MLEnvironmentFactory.get(sessionId).getExecutionEnvironment();
        HiveBatchSource hiveTableSource = getHiveBatchSource(tableName, parameter);
        DataSet<RowData> dataSet = hiveTableSource.getDataSet(env);
        TableSchema schema = hiveTableSource.getTableSchema();
        final DataType[] dataTypes = schema.getFieldDataTypes();
        DataSet<Row> rows = dataSet.map(new RowDataToRow(dataTypes));
        Table tbl = DataSetConversionUtil.toTable(sessionId, rows, schema);
        if (getPartitionCols(tableName).size() > 0) { // remove static partition columns
            String[] fieldNames = getColNames(tableName);
            tbl = tbl.select(Strings.join(fieldNames, ","));
        }
        return tbl;
    }

    private static class RowDataToRow implements MapFunction<RowData, Row> {
        DataType[] dataTypes;

        RowDataToRow(DataType[] dataTypes) {
            this.dataTypes = dataTypes;
        }

        @Override
        public Row map(RowData baseRow) throws Exception {
            Row row = new Row(baseRow.getArity());
            for (int i = 0; i < baseRow.getArity(); i++) {
                Object o = RowData.get(baseRow, i, dataTypes[i].getLogicalType());
                if (o != null && o instanceof BinaryStringData) {
                    o = ((BinaryStringData) o).toString();
                }
                row.setField(i, o);
            }
            return row;
        }
    }

    @Override
    public Table getStreamTable(String tableName, Params parameter, Long sessionId) throws Exception {
        StreamExecutionEnvironment env = MLEnvironmentFactory.get(sessionId).getStreamExecutionEnvironment();MLEnvironmentFactory.get(sessionId).getStreamExecutionEnvironment();
        StreamTableEnvironment stenv = MLEnvironmentFactory.get(sessionId).getStreamTableEnvironment();
        HiveTableSource hiveTableSource = getHiveTableSource(tableName, parameter, stenv);
        DataStream<RowData> dataStream = hiveTableSource.getDataStream(env);
        TableSchema schema = hiveTableSource.getTableSchema();
        final DataType[] dataTypes = schema.getFieldDataTypes();
        DataStream<Row> rows = dataStream.map(new RowDataToRow(dataTypes));
        return DataStreamConversionUtil.toTable(sessionId, rows, schema);
    }

    private static class RowToRowData implements MapFunction<Row, RowData> {
        private DataType[] dataTypes;
        private String[] fieldNames;
        private DataFormatConverters.RowConverter rowConverter;
        private Row partitionRow;

        RowToRowData(DataType[] dataTypes, String[] fieldNames, Map<String, String> staticPartitionSpec) {
            this.dataTypes = dataTypes;
            this.fieldNames = fieldNames;
            this.rowConverter = new DataFormatConverters.RowConverter(dataTypes);
            partitionRow = new Row(staticPartitionSpec.size());
            for (int i = 0; i < staticPartitionSpec.size(); i += 1) {
                int index = fieldNames.length - staticPartitionSpec.size() + i;
                partitionRow.setField(i, staticPartitionSpec.get(fieldNames[index]));
            }
        }

        @Override
        public RowData map(Row row) throws Exception {
            return rowConverter.toInternal(Row.join(row, partitionRow));
        }
    }

    @Override
    public void sinkStream(String tableName, Table in, Params parameter, Long sessionId) {
        try {
            MLEnvironment mlEnv = MLEnvironmentFactory.get(sessionId);
            checkTableExistenceBeforeSink(tableName, in, parameter);
            HiveTableFactory factory = new HiveTableFactory(getCatalog().getHiveConf());
            CatalogTable catalogTable = getCatalogTable(tableName);

            // partition table need this to inform downstream to commit
            catalogTable.getOptions().put(SINK_PARTITION_COMMIT_POLICY_KIND.key(), "metastore,success-file");

            TableSinkFactoryContextImpl context = new TableSinkFactoryContextImpl(
                ObjectIdentifier.of(getCatalog().getName(), dbName, tableName),
                catalogTable,
                mlEnv.getStreamTableEnvironment().getConfig().getConfiguration(),
                false
            );
            HiveTableSink tableSink = (HiveTableSink) factory.createTableSink(context);

            TableSchema schema = tableSink.getTableSchema();
            final DataType[] dataTypes = schema.getFieldDataTypes();

            Map<String, String> staticPartitionSpec = getStaticPartitionSpec(parameter.get(HiveSinkParams.PARTITION));

            DataStream<Row> rowDataStream = DataStreamConversionUtil.fromTable(sessionId, in);
            DataStream<RowData> rowDataDataStream = rowDataStream.map(
                new RowToRowData(dataTypes, schema.getFieldNames(), staticPartitionSpec));

            tableSink.setStaticPartition(staticPartitionSpec);
            tableSink.setOverwrite(false);  // Streaming mode not support overwrite
            tableSink.consumeDataStream(rowDataDataStream);
        } catch (Exception e) {
            throw new RuntimeException("Fail to sink stream table:", e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private FileSystemOutputFormat<Row> getOutputFormatInterval(HiveTableSink tableSink) throws Exception {
        final JobConf jobConf = (JobConf) FieldUtils.readField(tableSink, "jobConf", true);
        final CatalogTable catalogTable = (CatalogTable) FieldUtils.readField(tableSink, "catalogTable", true);
        final ObjectIdentifier identifier = (ObjectIdentifier) FieldUtils.readField(tableSink, "identifier", true);
        final TableSchema tableSchema = (TableSchema) FieldUtils.readField(tableSink, "tableSchema", true);
        final String hiveVersion = (String) FieldUtils.readField(tableSink, "hiveVersion", true);
        final HiveShim hiveShim = (HiveShim) FieldUtils.readField(tableSink, "hiveShim", true);
        boolean overwrite = (boolean) FieldUtils.readField(tableSink, "overwrite", true);
        boolean dynamicGrouping = (boolean) FieldUtils.readField(tableSink, "dynamicGrouping", true);
        LinkedHashMap<String, String> staticPartitionSpec =
            (LinkedHashMap<String, String>) FieldUtils.readField(tableSink, "staticPartitionSpec", true);

        String[] partitionColumns = catalogTable.getPartitionKeys().toArray(new String[0]);
        String dbName = identifier.getDatabaseName();
        String tableName = identifier.getObjectName();

        try (HiveMetastoreClientWrapper client = HiveMetastoreClientFactory.create(
            new HiveConf(jobConf, HiveConf.class), hiveVersion)) {
            org.apache.hadoop.hive.metastore.api.Table table = client.getTable(dbName, tableName);
            StorageDescriptor sd = table.getSd();

            Constructor<HiveTableMetaStoreFactory> hiveTableMetaStoreFactoryConstructor =
                HiveTableMetaStoreFactory.class.getDeclaredConstructor(JobConf.class, String.class, String.class, String.class);
            hiveTableMetaStoreFactoryConstructor.setAccessible(true);
            HiveTableMetaStoreFactory msFactory = hiveTableMetaStoreFactoryConstructor.newInstance(
                jobConf, hiveVersion, dbName, tableName);

            Constructor<HadoopFileSystemFactory> hadoopFileSystemFactoryConstructor =
                HadoopFileSystemFactory.class.getDeclaredConstructor(JobConf.class);
            hadoopFileSystemFactoryConstructor.setAccessible(true);
            HadoopFileSystemFactory fsFactory = hadoopFileSystemFactoryConstructor.newInstance(jobConf);

            Class hiveOutputFormatClz = hiveShim.getHiveOutputFormatClass(
                Class.forName(sd.getOutputFormat()));
            boolean isCompressed = jobConf.getBoolean(HiveConf.ConfVars.COMPRESSRESULT.varname, false);
            HiveWriterFactory recordWriterFactory = new HiveWriterFactory(
                jobConf,
                hiveOutputFormatClz,
                sd.getSerdeInfo(),
                tableSchema,
                partitionColumns,
                HiveReflectionUtils.getTableMetadata(hiveShim, table),
                hiveShim,
                isCompressed);
            String extension = Utilities.getFileExtension(jobConf, isCompressed,
                (HiveOutputFormat<?, ?>) hiveOutputFormatClz.newInstance());
            OutputFileConfig outputFileConfig = OutputFileConfig.builder()
                .withPartPrefix("part-" + UUID.randomUUID().toString())
                .withPartSuffix(extension == null ? "" : extension)
                .build();

            FileSystemOutputFormat.Builder<Row> builder = new FileSystemOutputFormat.Builder<>();

            Constructor<HiveRowPartitionComputer> hiveRowPartitionComputerConstructor =
                HiveRowPartitionComputer.class.getDeclaredConstructor(
                    HiveShim.class,
                    String.class,
                    String[].class,
                    DataType[].class,
                    String[].class
                );
            hiveRowPartitionComputerConstructor.setAccessible(true);

            builder.setPartitionComputer(hiveRowPartitionComputerConstructor.newInstance(
                hiveShim,
                jobConf.get(
                    HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
                    HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal),
                tableSchema.getFieldNames(),
                tableSchema.getFieldDataTypes(),
                partitionColumns));
            builder.setDynamicGrouped(dynamicGrouping);
            builder.setPartitionColumns(partitionColumns);
            builder.setFileSystemFactory(fsFactory);
            builder.setFormatFactory(new HiveOutputFormatFactory(recordWriterFactory));
            builder.setMetaStoreFactory(
                msFactory);
            builder.setOverwrite(overwrite);
            builder.setStaticPartitions(staticPartitionSpec);
            builder.setTempPath(new org.apache.flink.core.fs.Path(
                toStagingDir(sd.getLocation(), jobConf)));
            builder.setOutputFileConfig(outputFileConfig);
            return builder.build();
        }
    }

    // get a staging dir associated with a final dir
    private String toStagingDir(String finalDir, Configuration conf) throws IOException {
        String res = finalDir;
        if (!finalDir.endsWith(org.apache.hadoop.fs.Path.SEPARATOR)) {
            res += org.apache.hadoop.fs.Path.SEPARATOR;
        }
        // TODO: may append something more meaningful than a timestamp, like query ID
        res += ".staging_" + System.currentTimeMillis();
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(res);
        FileSystem fs = path.getFileSystem(conf);
        Preconditions.checkState(fs.exists(path) || fs.mkdirs(path), "Failed to create staging dir " + path);
        fs.deleteOnExit(path);
        return res;
    }

    @Override
    public void sinkBatch(String tableName, Table in, Params parameter, Long sessionId) {
        try {
            MLEnvironment mlEnv = MLEnvironmentFactory.get(sessionId);
            checkTableExistenceBeforeSink(tableName, in, parameter);
            HiveTableFactory factory = new HiveTableFactory(getCatalog().getHiveConf());
            CatalogTable catalogTable = getCatalogTable(tableName);

            TableSinkFactoryContextImpl context = new TableSinkFactoryContextImpl(
                ObjectIdentifier.of(getCatalog().getName(), dbName, tableName),
                catalogTable,
                mlEnv.getStreamTableEnvironment().getConfig().getConfiguration(),
                false
            );
            HiveTableSink tableSink = (HiveTableSink) factory.createTableSink(context);

            tableSink.setStaticPartition(getStaticPartitionSpec(parameter.get(HiveSinkParams.PARTITION)));
            tableSink.setOverwrite(true);
            FileSystemOutputFormat<Row> outputFormat = getOutputFormatInterval(tableSink);
            BatchOperator.fromTable(in).getDataSet().output(outputFormat).name("hive_sink_" + tableName);
        } catch (Exception e) {
            throw new RuntimeException("Fail to sink batch table:", e);
        }
    }

    @Override
    public RichOutputFormat createFormat(String tableName, TableSchema schema) {
        throw new UnsupportedOperationException("not supported.");
    }
}
