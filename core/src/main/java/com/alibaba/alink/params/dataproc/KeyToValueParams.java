package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.params.shared.HasNumThreads;
import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

public interface KeyToValueParams<T> extends
	HasSelectedCol <T>,
	HasReservedColsDefaultAsNull <T>,
	HasOutputColDefaultAsNull <T>, HasNumThreads <T> {

	ParamInfo <String> MAP_KEY_COL = ParamInfoFactory.createParamInfo("mapKeyCol", String.class)
		.setDescription("the name of the key column in map data table.")
		.setRequired()
		.build();

	default String getMapKeyCol() {
		return get(MAP_KEY_COL);
	}

	default T setMapKeyCol(String value) {
		return set(MAP_KEY_COL, value);
	}

	ParamInfo <String> MAP_VALUE_COL = ParamInfoFactory.createParamInfo("mapValueCol", String.class)
		.setDescription("the name of the value column in map data table.")
		.setRequired()
		.build();

	default String getMapValueCol() {
		return get(MAP_VALUE_COL);
	}

	default T setMapValueCol(String value) {
		return set(MAP_VALUE_COL, value);
	}

}
