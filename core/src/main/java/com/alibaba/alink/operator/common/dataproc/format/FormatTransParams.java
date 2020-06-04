package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

public interface FormatTransParams {

	ParamInfo <FormatType> FROM_FORMAT = ParamInfoFactory
		.createParamInfo("fromFormat", FormatType.class)
		.setDescription("the format type of trans from")
		.setRequired()
		.build();

	ParamInfo <FormatType> TO_FORMAT = ParamInfoFactory
		.createParamInfo("toFormat", FormatType.class)
		.setDescription("the format type of trans to")
		.setRequired()
		.build();

}