package com.alibaba.alink.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.common.annotation.DescCn;
import com.alibaba.alink.common.annotation.NameCn;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;

/**
 * Trait for parameter weightColName.
 */
@ParamSelectColumnSpec(name = "weightCol", allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
public interface HasWeightColDefaultAsNull<T> extends WithParams <T> {
	@NameCn("权重列名")
	@DescCn("权重列对应的列名")
	ParamInfo <String> WEIGHT_COL = ParamInfoFactory
		.createParamInfo("weightCol", String.class)
		.setDescription("Name of the column indicating weight")
		.setAlias(new String[] {"weightColName", "value"})
		.setHasDefaultValue(null)
		.build();

	default String getWeightCol() {return get(WEIGHT_COL);}

	default T setWeightCol(String colName) {return set(WEIGHT_COL, colName);}
}
