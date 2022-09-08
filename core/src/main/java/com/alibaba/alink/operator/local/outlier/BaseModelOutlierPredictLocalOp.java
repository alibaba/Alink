package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.local.utils.ModelMapLocalOp;
import com.alibaba.alink.params.outlier.ModelOutlierParams;

@Internal
public class BaseModelOutlierPredictLocalOp<T extends BaseModelOutlierPredictLocalOp <T>>
	extends ModelMapLocalOp <T> implements ModelOutlierParams <T> {

	public BaseModelOutlierPredictLocalOp(
		TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder, Params params) {
		super(mapperBuilder, params);
	}

}