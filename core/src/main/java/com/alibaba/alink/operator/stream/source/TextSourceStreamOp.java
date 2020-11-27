package com.alibaba.alink.operator.stream.source;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.params.io.TextSourceParams;

/**
 * Stream source that reads text lines.
 */
@IoOpAnnotation(name = "text", ioType = IOType.SourceStream)
public final class TextSourceStreamOp extends BaseSourceStreamOp <TextSourceStreamOp>
	implements TextSourceParams <TextSourceStreamOp> {

	private static final long serialVersionUID = 6460980977698094090L;

	public TextSourceStreamOp() {
		this(new Params());
	}

	public TextSourceStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(TextSourceStreamOp.class), params);
	}

	@Override
	public Table initializeDataSource() {
		return new CsvSourceStreamOp()
			.setMLEnvironmentId(getMLEnvironmentId())
			.setFilePath(getFilePath())
			.setFieldDelimiter("\n")
			.setQuoteChar(null)
			.setIgnoreFirstLine(getIgnoreFirstLine())
			.setSchemaStr(getTextCol() + " string")
			.getOutputTable();
	}
}
