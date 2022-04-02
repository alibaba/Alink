package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.function.TriFunction;

import com.alibaba.alink.common.mapper.ModelMapper;
import com.alibaba.alink.pipeline.MapModel;

public abstract class BaseModelOutlierModel<T extends BaseModelOutlierModel <T>> extends MapModel <T>
	implements ModelOutlierParams <T> {

	public BaseModelOutlierModel(TriFunction <TableSchema, TableSchema, Params, ModelMapper> mapperBuilder,
								 Params params) {
		super(mapperBuilder, params);
	}

}
