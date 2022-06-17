package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.io.HBaseConfigParams;
import com.alibaba.alink.params.io.HBaseParams;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface LookupHBaseParams<T> extends
	HBaseConfigParams <T>,
	HBaseParams <T>,
	HasReservedColsDefaultAsNull <T> {

	@NameCn("Schema")
	@DescCn("Schema。格式为\"colname coltype[, colname2, coltype2[, ...]]\"，例如\"f0 string, f1 bigint, f2 double\"")
	ParamInfo <String> OUTPUT_SCHEMA_STR = ParamInfoFactory
		.createParamInfo("outputSchemaStr", String.class)
		.setDescription("Formatted schema for lookup result")
		.setRequired()
		.build();

	default String getOutputSchemaStr() {
		return get(OUTPUT_SCHEMA_STR);
	}

	default T setOutputSchemaStr(String value) {
		return set(OUTPUT_SCHEMA_STR, value);
	}
}
