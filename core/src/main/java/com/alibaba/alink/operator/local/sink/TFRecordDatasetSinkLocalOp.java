package com.alibaba.alink.operator.local.sink;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.filesystem.TFRecordDatasetUtils.TFRecordDatasetOutputFormat;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.TFRecordDatasetSinkParams;

/**
 * Sink batch op data to a file system with TFRecordDataset format.
 */
@NameCn("TFRecordDataset文件导出")
public final class TFRecordDatasetSinkLocalOp extends BaseSinkLocalOp <TFRecordDatasetSinkLocalOp>
	implements TFRecordDatasetSinkParams <TFRecordDatasetSinkLocalOp> {

	public TFRecordDatasetSinkLocalOp() {
		this(new Params());
	}

	public TFRecordDatasetSinkLocalOp(Params params) {
		super(params);
	}

	@Override
	protected void linkFromImpl(LocalOperator <?>... inputs) {
		// Not use parent implementation, as original values are required.
		sinkFrom(checkAndGetFirst(inputs));
	}

	@Override
	public TFRecordDatasetSinkLocalOp sinkFrom(LocalOperator <?> in) {
		WriteMode mode = getOverwriteSink()
			? WriteMode.OVERWRITE
			: WriteMode.NO_OVERWRITE;
		TFRecordDatasetOutputFormat format = new TFRecordDatasetOutputFormat(
			getFilePath(),
			TableUtil.schema2SchemaStr(in.getSchema()),
			mode);
		output(in.getOutputTable().getRows(), format, getNumFiles());
		return this;
	}
}
