package com.alibaba.alink.params.shared.clustering;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.operator.common.clustering.agnes.Linkage;
import com.alibaba.alink.params.ParamUtil;

public interface HasLinkage<T> extends WithParams <T> {

	@NameCn("类的聚合方式")
	@DescCn("类的聚合方式")
	ParamInfo <Linkage> LINKAGE = ParamInfoFactory
		.createParamInfo("linkage", Linkage.class)
		.setDescription("linkage")
		.setHasDefaultValue(Linkage.MIN)
		.build();

	default Linkage getLinkage() {
		return get(LINKAGE);
	}

	default T setLinkage(Linkage value) {
		return set(LINKAGE, value);
	}

	default T setLinkage(String value) {
		return set(LINKAGE, ParamUtil.searchEnum(LINKAGE, value));
	}
}
