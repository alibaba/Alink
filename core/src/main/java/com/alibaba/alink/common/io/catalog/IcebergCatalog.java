package com.alibaba.alink.common.io.catalog;

import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.io.annotations.CatalogAnnotation;
import com.alibaba.alink.common.io.catalog.plugin.IcebergClassLoaderFactory;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.LocalFileSystem;
import com.alibaba.alink.common.utils.DataSetConversionUtil;
import com.alibaba.alink.common.utils.DataStreamConversionUtil;
import com.alibaba.alink.operator.common.io.reader.HttpFileSplitReader;
import com.alibaba.alink.params.io.IcebergCatalogParams;
import com.alibaba.alink.params.io.IcebergCatalogParams;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
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
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CatalogAnnotation(name = "iceberg")
public class IcebergCatalog extends BaseCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalog.class);

  private Catalog internal;
  private static final String CATALOG_HIVE_CONF_DIR = "hive-conf-dir";
  private static final String WAREHOUSE = "warehouse";
  private static final String HIVE_URI = "uri";
  private static final String CATALOG_TYPE = "catalog-type";
  private static final String CATALOG_DEFAULT_DATABASE = "default-database";
  private final IcebergClassLoaderFactory icebergClassLoaderFactory;


  public IcebergCatalog(String catalogName, String defaultDatabase, String icebergVersion, String warehouse, String uri) {
    this(catalogName, defaultDatabase, icebergVersion, null, "hive", warehouse, uri);
  }

  public IcebergCatalog(String catalogName, String defaultDatabase, String icebergVersion, FilePath hiveConfDir,
                        String catalogType, String warehouse, String uri) {

    this(new Params()
        .set(IcebergCatalogParams.CATALOG_NAME, catalogName)
        .set(IcebergCatalogParams.DEFAULT_DATABASE, defaultDatabase == null ? "default" : defaultDatabase)
        .set(IcebergCatalogParams.HIVE_CONF_DIR, hiveConfDir.serialize())
        .set(IcebergCatalogParams.PLUGIN_VERSION, icebergVersion)
        .set(IcebergCatalogParams.CATALOG_TYPE, catalogType)
        .set(IcebergCatalogParams.WAREHOUSE, warehouse)
        .set(IcebergCatalogParams.HIVE_URI, uri)
    );
  }

  public IcebergCatalog(Params params) {
    super(params);
    icebergClassLoaderFactory = new IcebergClassLoaderFactory(getParams().get(IcebergCatalogParams.PLUGIN_VERSION));
  }

  @Override
  public Table sourceStream(ObjectPath objectPath, Params params, Long sessionId) {
    Catalog catalog = loadCatalog();
    TableSchema schema = null;
    try {
      schema = catalog.getTable(objectPath).getSchema();
    } catch (TableNotExistException e) {
      LOG.error("get iceberg table schema error.", e);
    }

    Tuple2<TypeInformation<RowData>, RichInputFormat<RowData, InputSplit>> tuple2
        = createInputFormat(MLEnvironmentFactory.get(sessionId).getStreamExecutionEnvironment(),
        objectPath, catalog, icebergClassLoaderFactory);

    DataStream<Row> dataStream = MLEnvironmentFactory
        .get(sessionId)
        .getStreamExecutionEnvironment()
        .createInput(tuple2.f1, tuple2.f0)
        .map(new RowDataToRow(schema.getFieldDataTypes()));

    return DataStreamConversionUtil.toTable(sessionId, dataStream, schema);
  }

  @Override
  public void sinkStream(ObjectPath objectPath, Table in, Params params, Long sessionId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table sourceBatch(ObjectPath objectPath, Params params, Long sessionId) {

    Catalog catalog = loadCatalog();
    TableSchema schema = null;
    try {
      schema = catalog.getTable(objectPath).getSchema();
    } catch (TableNotExistException e) {
      e.printStackTrace();
    }

    Tuple2<TypeInformation<RowData>, RichInputFormat<RowData, InputSplit>> tuple2
        = createInputFormat(MLEnvironmentFactory.get(sessionId).getStreamExecutionEnvironment(),
        objectPath, catalog, icebergClassLoaderFactory);

    DataSet<Row> ds = MLEnvironmentFactory.get(sessionId)
        .getExecutionEnvironment()
        .createInput(tuple2.f1, tuple2.f0)
        .map(new IcebergCatalog.RowDataToRow(schema.getFieldDataTypes()));

    return DataSetConversionUtil.toTable(sessionId, ds, schema);
  }

  private static class RowDataToRow implements MapFunction<RowData, Row> {
    private static final long serialVersionUID = -2751018757273958023L;

    DataType[] dataTypes;

    RowDataToRow(DataType[] dataTypes) {
      this.dataTypes = dataTypes;
    }

    @Override
    public Row map(RowData baseRow) throws Exception {
      Row row = new Row(baseRow.getArity());
      for (int i = 0; i < baseRow.getArity(); i++) {
        if (baseRow.isNullAt(i)) {
          row.setField(i, null);
        } else {
          Object o = RowData
              .createFieldGetter(dataTypes[i].getLogicalType(), i)
              .getFieldOrNull(baseRow);

          if (o instanceof BinaryStringData) {
            o = o.toString();
          } else if (o instanceof DecimalData) {
            o = ((DecimalData) o).toBigDecimal();
          }

          row.setField(i, o);
        }
      }
      return row;
    }
  }

  @Override
  public void sinkBatch(ObjectPath objectPath, Table in, Params params, Long sessionId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void open() throws CatalogException {
    icebergClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().open());
  }

  private Catalog loadCatalog() {
    if (internal == null) {
      internal = icebergClassLoaderFactory
          .doAsThrowRuntime(() -> {
            Catalog catalog = createCatalog(getParams(), Thread.currentThread().getContextClassLoader());
            catalog.open();

            return catalog;
          });
    }

    return internal;
  }


  private Catalog createCatalog(Params params, ClassLoader classLoader) {
    String catalogName = params.get(IcebergCatalogParams.CATALOG_NAME);

    CatalogFactory factory = createCatalogFactory(classLoader);
    Map<String, String> properties = new HashMap<>();

    String catalogType = params.get(IcebergCatalogParams.CATALOG_TYPE);
    properties.put(CATALOG_TYPE, catalogType);
    if ("hive".equals(catalogType)) {
      if (params.contains(IcebergCatalogParams.HIVE_CONF_DIR)) {
        String localHiveConfDir;
        try {
          localHiveConfDir = downloadHiveConf(
              FilePath.deserialize(params.get(IcebergCatalogParams.HIVE_CONF_DIR))
          );
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
        properties.put(CATALOG_HIVE_CONF_DIR, localHiveConfDir);
      } else {
        properties.put(HIVE_URI, params.get(IcebergCatalogParams.HIVE_URI));
        properties.put(WAREHOUSE, params.get(IcebergCatalogParams.WAREHOUSE));
      }
    } else {
      properties.put(WAREHOUSE, params.get(IcebergCatalogParams.WAREHOUSE));
    }


    if (params.get(IcebergCatalogParams.DEFAULT_DATABASE) != null) {
      properties.put(CATALOG_DEFAULT_DATABASE, params.get(IcebergCatalogParams.DEFAULT_DATABASE));
    }

    return factory.createCatalog(catalogName, properties);
  }

  public static String downloadHiveConf(FilePath hiveConfDir) throws IOException {
    return downloadFolder(hiveConfDir, "hive-site.xml");
  }

  public static String downloadFolder(FilePath folder, String... files) throws IOException {
    // local
    if (folder.getFileSystem() instanceof LocalFileSystem) {
      return folder.getPathStr();
    }

    File localConfDir = new File(System.getProperty("java.io.tmpdir"), FileUtils.getRandomFilename(""));
    String scheme = folder.getPath().toUri().getScheme();

    if (!localConfDir.mkdir()) {
      throw new RuntimeException("Could not create the dir " + localConfDir.getAbsolutePath());
    }

    if (scheme != null && (scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
      for (String path : files) {
        try (HttpFileSplitReader reader = new HttpFileSplitReader(folder.getPathStr() + "/" + path)) {
          long fileLen = reader.getFileLength();
          reader.open(null, 0, fileLen);

          int offset = 0;
          byte[] buffer = new byte[1024];

          try (FileOutputStream outputStream = new FileOutputStream(
              Paths.get(localConfDir.getPath(), path).toFile())) {
            while (offset < fileLen) {
              int len = reader.read(buffer, offset, 1024);
              outputStream.write(buffer, offset, len);
              offset += len;
            }
          }

        } catch (FileNotFoundException exception) {
          // pass
        }
      }
    } else {
      for (String path : files) {
        // file system
        if (!folder.getFileSystem().exists(new Path(folder.getPath(), path))) {
          continue;
        }

        try (FSDataInputStream inputStream = folder.getFileSystem().open(
            new Path(folder.getPath(), path));
             FileOutputStream outputStream = new FileOutputStream(
                 Paths.get(localConfDir.getPath(), path).toFile())) {
          IOUtils.copy(inputStream, outputStream);
        }
      }
    }

    return localConfDir.getAbsolutePath();
  }

  public static CatalogFactory createCatalogFactory(ClassLoader classLoader) {
    try {
      return (CatalogFactory) classLoader
          .loadClass("org.apache.iceberg.flink.FlinkCatalogFactory")
          .getConstructor()
          .newInstance();
    } catch (ClassNotFoundException | NoSuchMethodException
        | InstantiationException | IllegalAccessException | InvocationTargetException e) {

      throw new RuntimeException("Could not find the iceberg catelog factory.", e);
    }
  }


  @Override
  public void close() throws CatalogException {
    icebergClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().close());
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    return icebergClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().listDatabases());
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
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
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    return icebergClassLoaderFactory.doAsThrowRuntime(() -> loadCatalog().tableExists(tablePath));
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) throws TableNotExistException, TableAlreadyExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters) throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists) throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
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
  public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException, TablePartitionedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  private Tuple2<TypeInformation<RowData>, RichInputFormat<RowData, InputSplit>> createInputFormat(
      StreamExecutionEnvironment senv, ObjectPath objectPath, Catalog catalog, IcebergClassLoaderFactory factory) {

    return factory.doAsThrowRuntime(() -> {
      Class<?> inputOutputFormat = Class.forName(
          "org.apache.iceberg.flink.InputOutputFormat",
          true, Thread.currentThread().getContextClassLoader()
      );

      Method method = inputOutputFormat.getMethod("createInputFormat", StreamExecutionEnvironment.class, Catalog.class, ObjectPath.class);
      Tuple2<TypeInformation<RowData>, RichInputFormat<RowData, InputSplit>> internalRet =
          (Tuple2<TypeInformation<RowData>, RichInputFormat<RowData, InputSplit>>) method.invoke(null, senv, catalog, objectPath);

      return internalRet;
    });
  }
}
