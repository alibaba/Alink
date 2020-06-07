package com.alibaba.alink.common.io;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.DBAnnotation;
import com.alibaba.alink.common.io.table.BaseDbTable;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.HiveDBParams;
import com.alibaba.alink.params.io.HiveSinkParams;
import com.alibaba.alink.params.io.HiveSourceParams;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connectors.hive.HiveTableFactory;
import org.apache.flink.connectors.hive.HiveTableSink;
import org.apache.flink.connectors.hive.HiveTableSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.*;

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
        throw new IllegalArgumentException("not supported.");
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

    private HiveTableSource getHiveTableSource(String tableName, Params parameter) throws Exception {
        ObjectPath objectPath = ObjectPath.fromString(dbName + "." + tableName);
        CatalogTable catalogTable = getCatalogTable(tableName);
        HiveTableFactory factory = new HiveTableFactory(getCatalog().getHiveConf());
        HiveTableSource hiveTableSource = (HiveTableSource) factory.createTableSource(objectPath, catalogTable);
        String partitionSpecsStr = parameter.get(HiveSourceParams.PARTITIONS);
        if (!StringUtils.isNullOrWhitespaceOnly(partitionSpecsStr)) {
            String[] partitionSpecs = partitionSpecsStr.split(",");
            hiveTableSource.getPartitions(); // FIXME: This is a workaround to a bug in HiveTableSource in Flink 1.9
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
        DataSet<BaseRow> dataSet = hiveTableSource.getDataSet(env);
        TableSchema schema = hiveTableSource.getTableSchema();
        final DataType[] dataTypes = schema.getFieldDataTypes();
        DataSet<Row> rows = dataSet.map(new BaseRowToRow(dataTypes));
        Table tbl = DataSetConversionUtil.toTable(sessionId, rows, schema);
        if (getPartitionCols(tableName).size() > 0) { // remove static partition columns
            String[] fieldNames = getColNames(tableName);
            tbl = tbl.select(Strings.join(fieldNames, ","));
        }
        return tbl;
    }

    private static class BaseRowToRow implements MapFunction<BaseRow, Row> {
        DataType[] dataTypes;

        BaseRowToRow(DataType[] dataTypes) {
            this.dataTypes = dataTypes;
        }

        @Override
        public Row map(BaseRow baseRow) throws Exception {
            Row row = new Row(baseRow.getArity());
            for (int i = 0; i < baseRow.getArity(); i++) {
                Object o = TypeGetterSetters.get(baseRow, i, dataTypes[i].getLogicalType());
                if (o != null && o instanceof BinaryString) {
                    o = ((BinaryString) o).toString();
                }
                row.setField(i, o);
            }
            return row;
        }
    }

    @Override
    public Table getStreamTable(String tableName, Params parameter, Long sessionId) throws Exception {
        StreamExecutionEnvironment env = MLEnvironmentFactory.get(sessionId).getStreamExecutionEnvironment();
        HiveTableSource hiveTableSource = getHiveTableSource(tableName, parameter);
        DataStream<BaseRow> dataStream = hiveTableSource.getDataStream(env);
        TableSchema schema = hiveTableSource.getTableSchema();
        final DataType[] dataTypes = schema.getFieldDataTypes();
        DataStream<Row> rows = dataStream.map(new BaseRowToRow(dataTypes));
        return DataStreamConversionUtil.toTable(sessionId, rows, schema);
    }

    @Override
    public void sinkStream(String tableName, Table in, Params parameter, Long sessionId) {
        try {
            checkTableExistenceBeforeSink(tableName, in, parameter);
            HiveTableFactory factory = new HiveTableFactory(getCatalog().getHiveConf());
            ObjectPath objectPath = ObjectPath.fromString(dbName + "." + tableName);
            CatalogTable catalogTable = getCatalogTable(tableName);
            HiveTableSink tableSink = (HiveTableSink) factory.createTableSink(objectPath, catalogTable);
            tableSink.setStaticPartition(getStaticPartitionSpec(parameter.get(HiveSinkParams.PARTITION)));
            tableSink.setOverwrite(true);
            tableSink.emitDataStream(StreamOperator.fromTable(in).getDataStream());
        } catch (Exception e) {
            throw new RuntimeException("Fail to sink stream table:", e);
        }
    }

    @Override
    public void sinkBatch(String tableName, Table in, Params parameter, Long sessionId) {
        try {
            checkTableExistenceBeforeSink(tableName, in, parameter);
            HiveTableFactory factory = new HiveTableFactory(getCatalog().getHiveConf());
            ObjectPath objectPath = ObjectPath.fromString(dbName + "." + tableName);
            CatalogTable catalogTable = getCatalogTable(tableName);
            HiveTableSink tableSink = (HiveTableSink) factory.createTableSink(objectPath, catalogTable);
            tableSink.setStaticPartition(getStaticPartitionSpec(parameter.get(HiveSinkParams.PARTITION)));
            tableSink.setOverwrite(true);
            OutputFormat<Row> outputFormat = tableSink.getOutputFormat();
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
