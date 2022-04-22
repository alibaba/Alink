package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.params.outlier.Outlier4GroupedDataParams;
import com.alibaba.alink.pipeline.MapTransformer;

import java.util.function.BiFunction;

public abstract class BaseOutlier4GroupedData<T extends BaseOutlier4GroupedData <T>> extends MapTransformer <T>
	implements Outlier4GroupedDataParams <T> {

	public BaseOutlier4GroupedData(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(mapperBuilder, params);
	}

}
