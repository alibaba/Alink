package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.TsvSinkParams;

/**
 * Sink data to files in tab-separated values.
 */
@IoOpAnnotation(name = "tsv", ioType = IOType.SinkBatch)
@NameCn("TSV文件导出")
public final class TsvSinkBatchOp extends BaseSinkBatchOp <TsvSinkBatchOp>
	implements TsvSinkParams <TsvSinkBatchOp> {

	private static final long serialVersionUID = 6164316647487307444L;

	public TsvSinkBatchOp() {
		this(new Params());
	}

	public TsvSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(TsvSinkBatchOp.class), params);
	}

	@Override
	public TsvSinkBatchOp sinkFrom(BatchOperator<?> in) {
		in.link(
			new CsvSinkBatchOp()
				.setMLEnvironmentId(getMLEnvironmentId())
				.setFilePath(getFilePath())
				.setOverwriteSink(getOverwriteSink())
				.setFieldDelimiter("\t")
				.setQuoteChar(null)
				.setNumFiles(getNumFiles())
		);
		return this;
	}
}
