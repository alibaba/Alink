package org.apache.flink.connectors.hive;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;

import org.apache.hadoop.mapred.JobConf;

import java.util.List;
import java.util.Map;

public class InputOutputFormat {

	public static Tuple3 <TableSchema, TypeInformation <BaseRow>, RichInputFormat <BaseRow, ?>> createInputFormat(
		Catalog catalog, ObjectPath objectPath, CatalogTable table, List <Map <String, String>> remainingPartitions) {

		if (!(catalog instanceof HiveCatalog)) {
			throw new RuntimeException("Catalog should be hive catalog.");
		}

		HiveCatalog hiveCatalog = (HiveCatalog) catalog;

		HiveBatchAndStreamTableSource hiveTableSource = new HiveBatchAndStreamTableSource(
			new JobConf(hiveCatalog.getHiveConf()), objectPath, table
		);

		if (remainingPartitions != null) {
			hiveTableSource.applyPartitionPruning(remainingPartitions);
		}

		return Tuple3.of(
			hiveTableSource.getTableSchema(),
			(TypeInformation <BaseRow>) TypeInfoDataTypeConverter
				.fromDataTypeToTypeInfo(hiveTableSource.getProducedDataType()),
			hiveTableSource.getInputFormat()
		);
	}
}
