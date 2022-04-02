package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTimeIntervalDefaultAs1800<T> extends WithParams <T> {
	@NameCn("时间间隔")
	@DescCn("数据流流动过程中时间的间隔")
	ParamInfo <Integer> TIME_INTERVAL = ParamInfoFactory
		.createParamInfo("timeInterval", Integer.class)
		.setDescription("time interval")
		.setHasDefaultValue(1800)
		.build();

	default Integer getTimeInterval() {return get(TIME_INTERVAL);}

	default T setTimeInterval(Integer value) {return set(TIME_INTERVAL, value);}
}
