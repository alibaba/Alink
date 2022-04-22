package com.alibaba.alink.common.io.parquet.plugin;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.parquet.ParquetSourceFactory;
import com.alibaba.alink.params.io.ParquetSourceParams;

public class ParquetSourceFactoryImpl implements ParquetSourceFactory {
	@Override
	public Tuple2<RichInputFormat<Row, FileInputSplit>, TableSchema> createParquetSourceFunction(Params params) {
		FilePath filePath = FilePath.deserialize(params.get(ParquetSourceParams.FILE_PATH));

		TableSchema schema = null;
		try {
			 schema = ParquetUtil.getTableSchemaFromParquetFile(filePath);
		} catch (Exception e) {
			throw new IllegalArgumentException("Cannot read footer from parquet file");
		}
	
		GenericParquetInputFormat parquetInputFormat = new GenericParquetInputFormat(filePath);

		return Tuple2.of(parquetInputFormat, schema);
	}
}
