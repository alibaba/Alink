package com.alibaba.alink.params.dataproc;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.operator.batch.dataproc.AppendIdBatchOp;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Param: idColName.
 */
public interface HasIdCol<T> extends WithParams<T> {

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
