package com.alibaba.alink.common.io.parquet.plugin;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.parquet.ParquetSourceFactory;
import com.alibaba.alink.params.io.ParquetSourceParams;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.Serializable;

public class ParquetSourceFactoryImpl implements ParquetSourceFactory {
	@Override
	public Tuple2 <RichInputFormat <Row, FileInputSplit>, TableSchema> createParquetSourceFunction(Params params) {
		FilePath filePath = FilePath.deserialize(params.get(ParquetSourceParams.FILE_PATH));
		MessageType messageType = ParquetUtil.getReadSchemaFromParquetFile(filePath);
		RowTypeInfo rowTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(messageType);
		return Tuple2.of(new GenericParquetInputFormat(filePath),
			new TableSchema(rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes()));
	}

}
