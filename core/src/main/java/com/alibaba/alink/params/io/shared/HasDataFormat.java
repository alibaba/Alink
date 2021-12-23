package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.ParamUtil;

public interface HasDataFormat<T> extends WithParams <T> {
	/**
	 * @cn-name 数据格式
	 * @cn 数据格式。json,csv
	 */
	// "json", "csv"
	ParamInfo <DataFormat> DATA_FORMAT = ParamInfoFactory
		.createParamInfo("dataFormat", DataFormat.class)
		.setDescription("data format")
		.setRequired()
		.setAlias(new String[] {"type"})
		.build();

	default DataFormat getDataFormat() {
		return get(DATA_FORMAT);
	}

	default T setDataFormat(DataFormat value) {
		return set(DATA_FORMAT, value);
	}

	default T setDataFormat(String value) {
		return set(DATA_FORMAT, ParamUtil.searchEnum(DATA_FORMAT, value));
	}

	enum DataFormat {
		JSON,
		CSV
	}
}
