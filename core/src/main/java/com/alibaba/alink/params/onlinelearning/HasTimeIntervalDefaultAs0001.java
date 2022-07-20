package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasTimeIntervalDefaultAs0001<T> extends WithParams <T> {

	@NameCn("时间间隔")
	@DescCn("数据流流动过程中两条样本间的时间间隔，单位秒")
	ParamInfo <Double> TIME_INTERVAL = ParamInfoFactory
		.createParamInfo("timeInterval", Double.class)
		.setDescription("time interval between two samples.")
		.setHasDefaultValue(0.001)
		.build();

	default Double getTimeInterval() {return get(TIME_INTERVAL);}

	default T setTimeInterval(Double value) {return set(TIME_INTERVAL, value);}

	default T setTimeInterval(Integer value) {return set(TIME_INTERVAL, 1.0 * value);}
}
