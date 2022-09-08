package com.alibaba.alink.operator.local.outlier;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.annotation.Internal;
import com.alibaba.alink.common.mapper.Mapper;
import com.alibaba.alink.operator.local.utils.MapLocalOp;
import com.alibaba.alink.params.outlier.Outlier4GroupedDataParams;

import java.util.function.BiFunction;

@Internal
public class BaseOutlier4GroupedDataLocalOp<T extends BaseOutlier4GroupedDataLocalOp <T>> extends MapLocalOp <T>
	implements Outlier4GroupedDataParams <T> {

	public BaseOutlier4GroupedDataLocalOp(BiFunction <TableSchema, Params, Mapper> mapperBuilder, Params params) {
		super(mapperBuilder, params);
	}

}