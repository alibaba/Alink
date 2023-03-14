package com.alibaba.alink.operator.stream.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.params.io.TextSinkParams;

/**
 * StreamOperator to sink data a file in plain text lines.
 */
@IoOpAnnotation(name = "text", ioType = IOType.SinkStream)
@NameCn("Text文件导出")
@NameEn("Text Sink")
public final class TextSinkStreamOp extends BaseSinkStreamOp <TextSinkStreamOp>
	implements TextSinkParams <TextSinkStreamOp> {

	private static final long serialVersionUID = 8805027283170198476L;

	public TextSinkStreamOp() {
		this(new Params());
	}

	public TextSinkStreamOp(Params params) {
		super(AnnotationUtils.annotatedName(TextSinkStreamOp.class), params);
	}

	@Override
	public TextSinkStreamOp sinkFrom(StreamOperator<?> in) {
		TypeInformation <?>[] types = in.getSchema().getFieldTypes();
		if (types.length != 1 || Types.STRING != types[0]) {
			throw new IllegalArgumentException("The Input could only be a string type column.");
		}

		in.link(
			new CsvSinkStreamOp()
				.setMLEnvironmentId(getMLEnvironmentId())
				.setFilePath(getFilePath())
				.setOverwriteSink(getOverwriteSink())
				.setFieldDelimiter("\n")
				.setQuoteChar(null)
				.setNumFiles(getNumFiles())
		);
		return this;
	}
}
