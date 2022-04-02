package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.TsvSinkParams;

/**
 * StreamOperator to sink data a file in tab-separated values.
 */
@IoOpAnnotation(name = "tsv", ioType = IOType.SinkStream)
@NameCn("TSV文件导出")
public final class TsvSinkStreamOp extends BaseSinkStreamOp <TsvSinkStreamOp>
	implements TsvSinkParams <TsvSinkStreamOp> {

	private static final long serialVersionUID = -1680254628645285214L;

	public TsvSinkStreamOp() {
		this(new Params());
	}

	public TsvSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(TsvSinkStreamOp.class), params);
	}

	@Override
	public TsvSinkStreamOp sinkFrom(StreamOperator<?> in) {
		in.link(
			new CsvSinkStreamOp()
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
