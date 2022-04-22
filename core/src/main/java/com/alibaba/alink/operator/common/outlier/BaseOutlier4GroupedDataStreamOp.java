package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.stream.utils.MapStreamOp;
import com.alibaba.alink.params.outlier.Outlier4GroupedDataParams;

import java.util.function.BiFunction;

@Internal
public class BaseOutlier4GroupedDataStreamOp<T extends BaseOutlier4GroupedDataStreamOp <T>> extends MapStreamOp <T>
	implements Outlier4GroupedDataParams <T> {

	public BaseOutlier4GroupedDataStreamOp(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(mapperBuilder, params);
	}

}