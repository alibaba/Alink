package com.alibaba.alink.params.feature;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

/**
 * Params for autocross.
 */
public interface AutoCrossPredictParams<T> extends
	HasOutputCol <T>,
	HasReservedColsDefaultAsNull <T> {

	@NameCn("输出格式")
	@DescCn("输出格式")
	ParamInfo<OutputFormat> OUTPUT_FORMAT = ParamInfoFactory
		.createParamInfo("outputFormat", OutputFormat.class)
		.setDescription("Output format")
		.setHasDefaultValue(OutputFormat.Sparse)
		.build();

	default OutputFormat getOutputFormat() {
		return get(OUTPUT_FORMAT);
	}

	default T setOutputFormat(OutputFormat value) {
		return set(OUTPUT_FORMAT, value);
	}

	default T setOutputFormat(String value) {
		return set(OUTPUT_FORMAT, ParamUtil.searchEnum(OUTPUT_FORMAT, value));
	}

	enum OutputFormat {

		Dense,

		Sparse,

		Word
	}
}
