package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.TFRecordDatasetUtils.TFRecordDatasetOutputFormat;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.TFRecordDatasetSinkParams;

/**
 * Sink batch op data to a file system with TFRecordDataset format.
 */
@IoOpAnnotation(name = "tfrecord", ioType = IOType.SinkStream)
@NameCn("TFRecordDataset文件导出")
@NameEn("TFRecord Dataset Sink")
public final class TFRecordDatasetSinkStreamOp extends BaseSinkStreamOp <TFRecordDatasetSinkStreamOp>
	implements TFRecordDatasetSinkParams <TFRecordDatasetSinkStreamOp> {

	private static final long serialVersionUID = -432966262826183973L;
	private TableSchema schema;

	public TFRecordDatasetSinkStreamOp() {
		this(new Params());
	}

	public TFRecordDatasetSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(TFRecordDatasetSinkStreamOp.class), params);
	}

	@Override
	public TableSchema getSchema() {
		return schema;
	}

	@Override
	public TFRecordDatasetSinkStreamOp linkFrom(StreamOperator <?>... inputs) {
		// Not use parent implementation, as original values are required.
		return sinkFrom(checkAndGetFirst(inputs));
	}

	@Override
	public TFRecordDatasetSinkStreamOp sinkFrom(StreamOperator <?> in) {
		schema = in.getSchema();
		WriteMode mode = getOverwriteSink()
			? WriteMode.OVERWRITE
			: WriteMode.NO_OVERWRITE;
		TFRecordDatasetOutputFormat format =
			new TFRecordDatasetOutputFormat(getFilePath(), TableUtil.schema2SchemaStr(schema), mode);

		//noinspection deprecation
		in.getDataStream()
			.addSink(new OutputFormatSinkFunction <>(format))
			.name("TFRecordDatasetSink")
			.setParallelism(getNumFiles());
		return this;
	}
}
