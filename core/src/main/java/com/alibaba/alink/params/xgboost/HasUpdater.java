package com.alibaba.alink.params.xgboost;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

public interface HasUpdater<T> extends WithParams <T> {

	@NameCn("Updater")
	@DescCn("Updater")
	ParamInfo <String> UPDATER = ParamInfoFactory
		.createParamInfo("updater", String.class)
		.setDescription("A comma separated string defining the sequence of tree updaters to run, "
			+ "providing a modular way to construct and to modify the trees.")
		.setHasDefaultValue("grow_colmaker,prune")
		.build();

	default String getUpdater() {
		return get(UPDATER);
	}

	default T setUpdater(String updater) {
		return set(UPDATER, updater);
	}
}
