package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.TextSinkParams;

/**
 * Sink data to files in text lines.
 */
@IoOpAnnotation(name = "text", ioType = IOType.SinkBatch)
public final class TextSinkBatchOp extends BaseSinkBatchOp <TextSinkBatchOp>
	implements TextSinkParams <TextSinkBatchOp> {

	public TextSinkBatchOp() {
		this(new Params());
	}

	public TextSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(TextSinkBatchOp.class), params);
	}

	@Override
	public TextSinkBatchOp sinkFrom(BatchOperator in) {
		TypeInformation <?>[] types = in.getSchema().getFieldTypes();
		if (types.length != 1 || Types.STRING != types[0]) {
			throw new IllegalArgumentException("The Input could only be a string type column.");
		}

		in.link(
			new CsvSinkBatchOp()
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
