package com.alibaba.alink.operator.local.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.exceptions.AkIllegalArgumentException;
import com.alibaba.alink.operator.local.LocalOperator;
import com.alibaba.alink.params.io.TextSinkBatchParams;

/**
 * Sink data to files in text lines.
 */
@NameCn("Text文件导出")
public final class TextSinkLocalOp extends BaseSinkLocalOp <TextSinkLocalOp>
	implements TextSinkBatchParams <TextSinkLocalOp> {

	public TextSinkLocalOp() {
		this(new Params());
	}

	public TextSinkLocalOp(Params params) {
		super(params);
	}

	@Override
	public TextSinkLocalOp sinkFrom(LocalOperator <?> in) {
		TypeInformation <?>[] types = in.getSchema().getFieldTypes();
		if (types.length != 1 || Types.STRING != types[0]) {
			throw new AkIllegalArgumentException("The Input could only be a string type column.");
		}

		in.link(
			new CsvSinkLocalOp()
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
