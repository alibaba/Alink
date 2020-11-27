package com.alibaba.alink.params.dataproc.format;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying the name of the table column.
 */
public interface HasCsvCol<T> extends WithParams <T> {

	ParamInfo <String> CSV_COL = ParamInfoFactory
		.createParamInfo("csvCol", String.class)
		.setDescription("Name of the CSV column")
		.setRequired()
		.build();

	default String getCsvCol() {
		return get(CSV_COL);
	}

	default T setCsvCol(String colName) {
		return set(CSV_COL, colName);
	}
}
