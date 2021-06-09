package com.alibaba.alink.operator.common.feature.featurebuilder;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class WindowResColType {

	static class ResColType {}

	public static final TypeInformation <ResColType> RES_TYPE = TypeInformation.of(ResColType.class);

}
