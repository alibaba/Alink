package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasLatitudeCol<T> extends WithParams <T> {
	@NameCn("经度列名")
	@DescCn("经度列名")
	ParamInfo <String> LATITUDE_COL = ParamInfoFactory
		.createParamInfo("latitudeCol", String.class)
		.setDescription("latitude col name")
		.setRequired()
		.setAlias(new String[] {"latitudeColName"})
		.build();

	default String getLatitudeCol() {return get(LATITUDE_COL);}

	default T setLatitudeCol(String value) {return set(LATITUDE_COL, value);}
}
