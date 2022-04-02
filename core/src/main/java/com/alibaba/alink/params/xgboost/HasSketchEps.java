package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasSketchEps<T> extends WithParams <T> {

	@NameCn("SketchEps")
	@DescCn("SketchEps")
	ParamInfo <Double> SKETCH_EPS = ParamInfoFactory
		.createParamInfo("sketchEps", Double.class)
		.setDescription("This roughly translates into O(1 / sketch_eps) number of bins. "
			+ "Compared to directly select number of bins, this comes with theoretical guarantee with sketch "
			+ "accuracy.")
		.setHasDefaultValue(0.03)
		.build();

	default Double getSketchEps() {
		return get(SKETCH_EPS);
	}

	default T setSketchEps(Double sketchEps) {
		return set(SKETCH_EPS, sketchEps);
	}
}
