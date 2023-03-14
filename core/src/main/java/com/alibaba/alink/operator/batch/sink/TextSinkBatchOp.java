package com.alibaba.alink.operator.batch.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.NameEn;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.common.io.annotations.AnnotationUtils;
import com.alibaba.alink.common.io.annotations.IOType;
import com.alibaba.alink.common.io.annotations.IoOpAnnotation;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.params.io.TextSinkBatchParams;

/**
 * Sink data to files in text lines.
 */
@IoOpAnnotation(name = "text", ioType = IOType.SinkBatch)
@NameCn("Text文件导出")
@NameEn("Text Sink")
public final class TextSinkBatchOp extends BaseSinkBatchOp <TextSinkBatchOp>
	implements TextSinkBatchParams <TextSinkBatchOp> {

	private static final long serialVersionUID = -8765859539929096961L;

	public TextSinkBatchOp() {
		this(new Params());
	}

	public TextSinkBatchOp(Params params) {
		super(AnnotationUtils.annotatedName(TextSinkBatchOp.class), params);
	}

	@Override
	public TextSinkBatchOp sinkFrom(BatchOperator <?> in) {
		TypeInformation <?>[] types = in.getSchema().getFieldTypes();
		if (types.length != 1 || Types.STRING != types[0]) {
			throw new AkIllegalArgumentException("The Input could only be a string type column.");
		}

		in.link(
			new CsvSinkBatchOp()
				.setMLEnvironmentId(getMLEnvironmentId())
				.setFilePath(getFilePath())
				.setOverwriteSink(getOverwriteSink())
				.setFieldDelimiter("\n")
				.setQuoteChar(null)
				.setNumFiles(getNumFiles())
				.setPartitionCols(getPartitionCols())
		);

		return this;
	}
}
