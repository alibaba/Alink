package com.alibaba.alink.params.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.ParamUtil;

public interface HasBiFuncName<T> extends WithParams <T> {
	@NameCn("函数名字")
	@DescCn("函数操作名称, 可取minus(减),plus(加),dot(内积),merge(拼接),EuclidDistance(欧式距离),Cosine(cos值), ElementWiseMultiply(点乘).")
	ParamInfo <BiFuncName> BI_FUNC_NAME = ParamInfoFactory
		.createParamInfo("biFuncName", BiFuncName.class)
		.setDescription("the name of vecor function")
		.setRequired()
		.build();

	default BiFuncName getBiFuncName() {
		return get(BI_FUNC_NAME);
	}

	default T setBiFuncName(BiFuncName value) {
		return set(BI_FUNC_NAME, value);
	}

	default T setBiFuncName(String value) {
		return set(BI_FUNC_NAME, ParamUtil.searchEnum(BI_FUNC_NAME, value));
	}

	enum BiFuncName {
		Minus,
		Dot,
		Plus,
		Merge,
		EuclidDistance,
		Cosine,
		ElementWiseMultiply
	}
}