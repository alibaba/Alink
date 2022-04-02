package com.alibaba.alink.params.tensorflow.bert;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasCustomConfigJson<T> extends WithParams <T> {
	@NameCn("自定义参数")
	@DescCn("对应 https://github.com/alibaba/EasyTransfer/blob/master/easytransfer/app_zoo/app_config.py 中的config_json")
	ParamInfo <String> CUSTOM_CONFIG_JSON = ParamInfoFactory
		.createParamInfo("customConfigJson", String.class)
		.setDescription("Custom config in JSON format, corresponding to config_json in "
			+ "https://github.com/alibaba/EasyTransfer/blob/master/easytransfer/app_zoo/app_config.py")
		.setOptional()
		.build();

	default String getCustomConfigJson() {
		return get(CUSTOM_CONFIG_JSON);
	}

	default T setCustomConfigJson(String colName) {
		return set(CUSTOM_CONFIG_JSON, colName);
	}
}
