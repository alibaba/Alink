package com.alibaba.alink.common.io.parquet.plugin;

import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.formats.parquet.utils.ParquetRecordReader;
import org.apache.flink.formats.parquet.utils.RowReadSupport;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.alink.common.io.filesystem.BaseFileSystem;
import com.alibaba.alink.common.io.filesystem.FilePath;
import com.alibaba.alink.common.io.filesystem.copy.FileInputFormat;
import com.alibaba.alink.common.io.parquet.plugin.ParquetUtil.ParquetInputFile;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GenericParquetInputFormat extends FileInputFormat<Row>
	implements CheckpointableInputFormat<FileInputSplit, Tuple2<Long, Long>> {

	private static final Logger LOG = LoggerFactory.getLogger(GenericParquetInputFormat.class);

	/**
	 * The flag to specify whether to skip corrupted record.
	 */
	private boolean skipCorruptedRecord = false;

	/**
	 * The flag to track that the current split should be skipped.
	 */
	private boolean skipThisSplit = false;

	private MessageType readSchema;

	private FilePath parquetFilePath;
	private BaseFileSystem<?> fileSystem;

	private FilterPredicate filterPredicate;

	private transient Counter recordConsumed;

	private transient ParquetRecordReader<Row> parquetRecordReader;

	public GenericParquetInputFormat(FilePath filePath) {
		super(filePath.getPath(), filePath.getFileSystem());
		this.parquetFilePath = filePath;
		fileSystem = filePath.getFileSystem();
		// read whole parquet file as one file split
		this.unsplittable = true;
		// read schema
	}

	@Override
	public void configure(Configuration configuration) {
		super.configure(configuration);

		if (this.skipCorruptedRecord) {
			this.skipCorruptedRecord = configuration.getBoolean(PARQUET_SKIP_CORRUPTED_RECORD, false);
		}
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		// reset the flag when open a new split
		this.skipThisSplit = false;
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		InputFile inputFile = new ParquetInputFile(parquetFilePath);

		readSchema = ParquetUtil.getReadSchemaFromParquetFile(parquetFilePath);

		ParquetReadOptions options = ParquetReadOptions.builder().build();
		ParquetFileReader fileReader = new ParquetFileReader(inputFile, options);
		if (skipThisSplit) {
			LOG.warn(
				String.format(
					"Escaped the file split [%s] due to mismatch of file schema to expected result schema",
					split.getPath().toString()));
		} else {
			this.parquetRecordReader =
				new ParquetRecordReader<>(
					new RowReadSupport(),
					readSchema,
					filterPredicate == null
						? FilterCompat.NOOP
						: FilterCompat.get(filterPredicate));
			this.parquetRecordReader.initialize(fileReader, configuration);
			this.parquetRecordReader.setSkipCorruptedRecord(this.skipCorruptedRecord);

			if (this.recordConsumed == null) {
				this.recordConsumed =
					getRuntimeContext().getMetricGroup().counter("parquet-records-consumed");
			}

			LOG.debug(
				String.format(
					"Open ParquetInputFormat with FileInputSplit [%s]",
					split.getPath().toString()));
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		if (skipThisSplit) {
			return true;
		}

		return parquetRecordReader.reachEnd();
	}

	@Override
	public Row nextRecord(Row row) throws IOException {
		if (reachedEnd()) {
			return null;
		}

		recordConsumed.inc();
		return parquetRecordReader.nextRecord();
	}

	@Override
	public void close() throws IOException {
		if (parquetRecordReader != null) {
			parquetRecordReader.close();
		}
	}

	@Override
	public Tuple2<Long, Long> getCurrentState() {
		return parquetRecordReader.getCurrentReadPosition();
	}

	@Override
	public void reopen(FileInputSplit split, Tuple2<Long, Long> state) throws IOException {
		Preconditions.checkNotNull(split, "reopen() cannot be called on a null split.");
		Preconditions.checkNotNull(state, "reopen() cannot be called with a null initial state.");
		this.open(split);
		// seek to the read position in the split that we were at when the checkpoint was taken.
		parquetRecordReader.seek(state.f0, state.f1);
	}

	/**
	 * The config parameter which defines whether to skip corrupted record.
	 */
	public static final String PARQUET_SKIP_CORRUPTED_RECORD = "skip.corrupted.record";

}
