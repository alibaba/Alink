package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.copy.csv.TextOutputFormat;
import com.alibaba.alink.operator.common.io.csv.CsvUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.CsvSinkParams;

/**
 * Sink to local or HDFS files in CSV format.
 */
@IoOpAnnotation(name = "csv", ioType = IOType.SinkStream)
@NameCn("CSV文件导出")
@NameEn("CSV Sink")
public final class CsvSinkStreamOp extends BaseSinkStreamOp <CsvSinkStreamOp>
	implements CsvSinkParams <CsvSinkStreamOp> {

	private static final long serialVersionUID = 5959220866408439695L;
	private TableSchema schema;

	public CsvSinkStreamOp() {
		this(new Params());
	}

	public CsvSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(CsvSinkStreamOp.class), params);
	}

	@Override
	public TableSchema getSchema() {
		return this.schema;
	}

	@Override
	public CsvSinkStreamOp sinkFrom(StreamOperator<?> in) {
		this.schema = in.getSchema();

		final String filePath = getFilePath().getPathStr();
		final String fieldDelim = getFieldDelimiter();
		final String rowDelimiter = getRowDelimiter();
		final int numFiles = getNumFiles();
		final TypeInformation<?>[] types = in.getColTypes();
		final Character quoteChar = getQuoteChar();

		FileSystem.WriteMode writeMode;
		if (getOverwriteSink()) {
			writeMode = FileSystem.WriteMode.OVERWRITE;
		} else {
			writeMode = FileSystem.WriteMode.NO_OVERWRITE;
		}

		DataStream <String> output = in.getDataStream()
			.map(new CsvUtil.FormatCsvFunc(types, fieldDelim, quoteChar))
			.map(new CsvUtil.FlattenCsvFromRow(rowDelimiter));

		TextOutputFormat <String> tof = new TextOutputFormat <>(
			new Path(filePath), getFilePath().getFileSystem(), getRowDelimiter()
		);

		tof.setWriteMode(writeMode);

		output
			.addSink(
				new OutputFormatSinkFunction <>(tof)
			)
			.name("csv_sink")
			.setParallelism(numFiles);

		return this;
	}
}
