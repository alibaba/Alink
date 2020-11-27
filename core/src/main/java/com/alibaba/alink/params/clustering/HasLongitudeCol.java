package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasLongitudeCol<T> extends WithParams <T> {
	ParamInfo <String> LONGITUDE_COL = ParamInfoFactory
		.createParamInfo("longitudeCol", String.class)
		.setDescription("longitude col name")
		.setAlias(new String[] {"longitudeColName"})
		.setRequired()
		.build();

	default String getLongitudeCol() {return get(LONGITUDE_COL);}

	default T setLongitudeCol(String value) {return set(LONGITUDE_COL, value);}
}
