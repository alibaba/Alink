package com.alibaba.alink.params.io.shared;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface HasScanIntervalDefaultAs10<T> extends WithParams<T> {
	ParamInfo <Integer> SCAN_INTERVAL = ParamInfoFactory
		.createParamInfo("scanInterval", Integer.class)
		.setDescription("time interval")
		.setHasDefaultValue(10)
		.build();

	default Integer getScanInterval() {return get(SCAN_INTERVAL);}

	default T setScanInterval(Integer value) {return set(SCAN_INTERVAL, value);}
}
