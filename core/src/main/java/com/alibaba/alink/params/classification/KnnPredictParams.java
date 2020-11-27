package com.alibaba.alink.params.classification;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface KnnPredictParams<T> extends WithParams <T>,
	HasPredictionCol <T>,
	HasVectorColDefaultAsNull <T>,
	HasPredictionDetailCol <T>,
	HasReservedColsDefaultAsNull <T> {
	ParamInfo <Integer> K = ParamInfoFactory
		.createParamInfo("k", Integer.class)
		.setDescription("k")
		.setHasDefaultValue(10)
		.build();

	default Integer getK() {return get(K);}

	default T setK(Integer value) {return set(K, value);}
}
