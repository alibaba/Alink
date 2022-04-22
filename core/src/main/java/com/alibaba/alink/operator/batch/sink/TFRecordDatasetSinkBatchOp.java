package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.common.io.filesystem.TFRecordDatasetUtils.TFRecordDatasetOutputFormat;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.TFRecordDatasetSinkParams;

/**
 * Sink batch op data to a file system with TFRecordDataset format.
 */
@IoOpAnnotation(name = "tfrecord", ioType = IOType.SinkBatch)
@NameCn("TFRecordDataset文件导出")
public final class TFRecordDatasetSinkBatchOp extends BaseSinkBatchOp <TFRecordDatasetSinkBatchOp>
	implements TFRecordDatasetSinkParams <TFRecordDatasetSinkBatchOp> {

	public TFRecordDatasetSinkBatchOp() {
		this(new Params());
	}

	public TFRecordDatasetSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(TFRecordDatasetSinkBatchOp.class), params);
	}

	@Override
	public TFRecordDatasetSinkBatchOp linkFrom(BatchOperator <?>... inputs) {
		// Not use parent implementation, as original values are required.
		return sinkFrom(checkAndGetFirst(inputs));
	}

	@Override
	public TFRecordDatasetSinkBatchOp sinkFrom(BatchOperator <?> in) {
		WriteMode mode = getOverwriteSink()
			? WriteMode.OVERWRITE
			: WriteMode.NO_OVERWRITE;
		TFRecordDatasetOutputFormat format = new TFRecordDatasetOutputFormat(
			getFilePath(),
			TableUtil.schema2SchemaStr(in.getSchema()),
			mode);
		in.getDataSet()
			.output(format)
			.name("TFRecordDatasetSink")
			.setParallelism(getNumFiles());
		return this;
	}
}
