package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTunnelEndPoint<T> extends WithParams <T> {
	@NameCn("tunnelEndPoint")
	@DescCn("tunnel endpoint")
	ParamInfo <String> TUNNEL_END_POINT = ParamInfoFactory
		.createParamInfo("tunnelEndPoint", String.class)
		.setDescription("tunnel end point")
		.setHasDefaultValue(null)
		.build();

	default String getTunnelEndPoint() {
		return get(TUNNEL_END_POINT);
	}

	default T setTunnelEndPoint(String value) {
		return set(TUNNEL_END_POINT, value);
	}
}
