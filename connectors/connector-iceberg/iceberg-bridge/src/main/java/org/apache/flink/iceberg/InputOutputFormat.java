package org.apache.flink.iceberg;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.iceberg.source.FlinkInputFormat;
import org.apache.flink.iceberg.source.FlinkSource;
import org.apache.flink.iceberg.util.FlinkCompatibilityUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

import java.io.IOException;
import java.io.UncheckedIOException;

public class InputOutputFormat {

  public static Tuple3<TableSchema, TypeInformation<RowData>, RichInputFormat<RowData, ?>> createInputFormat(
      StreamExecutionEnvironment execEnv, Catalog catalog, ObjectPath objectPath) {

    if (!(catalog instanceof FlinkCatalog)) {
      throw new RuntimeException("Catalog should be iceberg catalog.");
    }

    TableLoader tableLoader = createTableLoader((FlinkCatalog) catalog, objectPath);

    Table table;
    Schema icebergSchema;
    tableLoader.open();
    try (TableLoader loader = tableLoader) {
      table = loader.loadTable();
      icebergSchema = table.schema();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    TableSchema tableSchema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(icebergSchema));

    TypeInformation<RowData> typeInfo = FlinkCompatibilityUtil.toTypeInfo(FlinkSchemaUtil.convert(icebergSchema));
    FlinkInputFormat flinkInputFormat = FlinkSource.forRowData()
        .env(execEnv)
        .tableLoader(tableLoader)
        .table(table)
        .buildFormat();
    return Tuple3.of(tableSchema, typeInfo, flinkInputFormat);
  }

  private static TableLoader createTableLoader(FlinkCatalog catalog, ObjectPath objectPath) {
    Preconditions.checkNotNull(catalog, "Flink catalog cannot be null");
    return TableLoader.fromCatalog(catalog.getCatalogLoader(), catalog.toIdentifier(objectPath));
  }
}
