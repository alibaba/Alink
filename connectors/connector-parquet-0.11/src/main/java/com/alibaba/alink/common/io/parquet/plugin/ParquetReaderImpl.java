package com.alibaba.alink.common.io.parquet.plugin;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.formats.parquet.utils.ParquetRecordReader;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.formats.parquet.utils.RowReadSupport;
import org.apache.flink.metrics.Counter;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.parquet.ParquetReaderFactory;
import com.alibaba.alink.common.io.parquet.plugin.ParquetUtil.ParquetInputFile;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

public class ParquetReaderImpl implements ParquetReaderFactory {
	private MessageType readSchema;
	private transient ParquetRecordReader <Row> parquetRecordReader = null;
	private FilterPredicate filterPredicate;
	private boolean skipCorruptedRecord = false;

	@Override
	public void open(FilePath filePath) {
		// reset the flag when open a new split
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		ParquetReadOptions options = ParquetReadOptions.builder().build();
		readSchema = ParquetUtil.getReadSchemaFromParquetFile(filePath);
		if (readSchema == null) {
			return;
		}
		InputFile inputFile = new ParquetInputFile(filePath);
		ParquetFileReader fileReader;
		try {
			fileReader = new ParquetFileReader(inputFile, options);
		} catch (Exception e) {
			return;
		}
		this.parquetRecordReader =
			new ParquetRecordReader <>(
				new RowReadSupport(),
				readSchema,
				filterPredicate == null
					? FilterCompat.NOOP
					: FilterCompat.get(filterPredicate));
		this.parquetRecordReader.initialize(fileReader, configuration);
		this.parquetRecordReader.setSkipCorruptedRecord(this.skipCorruptedRecord);
	}

	@Override
	public boolean reachedEnd() {
		if (parquetRecordReader != null) {
			try {
				return parquetRecordReader.reachEnd();
			} catch (Exception e) {
				throw new AkIllegalOperatorParameterException("cannot read parquet file");
			}
		} else {
			return true;
		}
	}

	@Override
	public Row nextRecord() {
		if (reachedEnd()) {
			return null;
		}
		return parquetRecordReader.nextRecord();
	}

	@Override
	public void close() {
		if (parquetRecordReader != null) {
			try {
				parquetRecordReader.close();
			} catch (Exception e) {
				throw new AkIllegalOperatorParameterException("cannot close parquet file");
			}
		}
	}

	@Override
	public TableSchema getTableSchemaFromParquetFile(FilePath filePath) {
		MessageType messageType = ParquetUtil.getReadSchemaFromParquetFile(filePath);
		RowTypeInfo schema = messageType == null ? new RowTypeInfo()
			: (RowTypeInfo) ParquetSchemaConverter.fromParquetType(messageType);
		return new TableSchema(schema.getFieldNames(), schema.getFieldTypes());
	}
}
