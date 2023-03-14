package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.clustering.DistanceType;
import com.alibaba.alink.params.ParamUtil;

/**
 * Params: Distance type for calculating the distance of vector.
 */
public interface HasDistanceType<T> extends WithParams <T> {
	@NameCn("距离度量方式")
	@DescCn("聚类使用的距离类型")
	ParamInfo <DistanceType> DISTANCE_TYPE = ParamInfoFactory
		.createParamInfo("distanceType", DistanceType.class)
		.setDescription("Distance type for clustering")
		.setHasDefaultValue(DistanceType.EUCLIDEAN)
		.setAlias(new String[] {"metric"})
		.build();

	default DistanceType getDistanceType() {return get(DISTANCE_TYPE);}

	default T setDistanceType(DistanceType value) {return set(DISTANCE_TYPE, value);}

	default T setDistanceType(String value) {
		return set(DISTANCE_TYPE, ParamUtil.searchEnum(DISTANCE_TYPE, value));
	}

}
