package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;

/**
 * Param: idColName.
 */
public interface HasIdCol<T> extends WithParams <T> {

	/**
	 * @cn-name ID列名
	 * @cn ID列名
	 */
	ParamInfo <String> ID_COL = ParamInfoFactory
		.createParamInfo("idCol", String.class)
		.setDescription("Id column name")
		.setHasDefaultValue(AppendIdBatchOp.appendIdColName)
		.setAlias(new String[] {"outputColName", "idColName"})
		.build();

	default String getIdCol() {
		return get(ID_COL);
	}

	default T setIdCol(String value) {
		return set(ID_COL, value);
	}
}
