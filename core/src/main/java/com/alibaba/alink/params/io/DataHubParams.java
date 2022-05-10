package com.alibaba.alink.params.io;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.params.io.shared.HasAccessId;
import com.alibaba.alink.params.io.shared.HasAccessKey;
import com.alibaba.alink.params.io.shared.HasCatalogName;
import com.alibaba.alink.params.io.shared.HasDefaultDatabase;
import com.alibaba.alink.params.io.shared.HasEndPoint;
import com.alibaba.alink.params.io.shared.HasPluginVersion;
import com.alibaba.alink.params.io.shared.HasProject;
import com.alibaba.alink.params.io.shared.HasStartTimeDefaultAsNull;

public interface DataHubParams<T> extends WithParams <T>,
	HasAccessId <T>, HasAccessKey <T>, HasProject <T>,
	HasEndPoint <T>, HasCatalogName <T>, HasDefaultDatabase <T>,
	HasPluginVersion <T>, HasStartTimeDefaultAsNull <T> {

	@NameCn("结束时间")
	@DescCn("结束时间。默认不结束")
	ParamInfo <String> END_TIME = ParamInfoFactory
		.createParamInfo("endTime", String.class)
		.setDescription("End time")
		.setHasDefaultValue(null)
		.build();

	default String getEndTime() {return get(END_TIME);}

	default T setEndTime(String value) {return set(END_TIME, value);}
}
