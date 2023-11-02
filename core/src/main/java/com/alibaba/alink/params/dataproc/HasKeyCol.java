package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;

/**
 * @author dota.zk
 * @date 28/12/2018
 */
public interface HasKeyCol<T> extends WithParams <T> {
	@NameCn("键值列")
	@DescCn("键值列")
	ParamInfo <String> KEY_COL = ParamInfoFactory
		.createParamInfo("keyCol", String.class)
		.setAlias(new String[] {"keyColName"})
		.setDescription("key col name")
		.setRequired()
		.build();

	default String getKeyCol() {return get(KEY_COL);}

	default T setKeyCol(String value) {return set(KEY_COL, value);}
}
