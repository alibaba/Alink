package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.utils.ModelMapStreamOp;

@Internal
public class BaseModelOutlierPredictStreamOp<T extends BaseModelOutlierPredictStreamOp <T>>
	extends ModelMapStreamOp <T> implements ModelOutlierParams <T> {

	public BaseModelOutlierPredictStreamOp(
		BatchOperator <?> model,
		TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
		Params params) {
		super(model, mapperBuilder, params);
	}

}