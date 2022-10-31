package com.alibaba.alink.params.similarity;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasMaxNumCandidates<T> extends WithParams <T> {
	@NameCn("最大备选点数量")
	@DescCn("从哈希桶中获得的最大备选点数量；该值越大，计算时间通常会越长，默认为无穷大，即获取所有哈希值一样的数据点")
	ParamInfo <Integer> MAX_NUM_CANDIDATES = ParamInfoFactory
		.createParamInfo("maxNumCandidates", Integer.class)
		.setDescription("maximum number of candidates retrieved from buckets")
		.setHasDefaultValue(Integer.MAX_VALUE)
		.build();

	default Integer getMaxNumCandidates() {return get(MAX_NUM_CANDIDATES);}

	default T setMaxNumCandidates(Integer value) {return set(MAX_NUM_CANDIDATES, value);}
}
