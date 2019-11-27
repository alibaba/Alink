package com.alibaba.alink.params.mapper;

import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasPredictionDetailCol;
import com.alibaba.alink.params.shared.colname.HasReservedCols;

/**
 * Params for RichModelMapper.
 */
public interface RichModelMapperParams<T> extends
	HasPredictionCol <T>,
	HasPredictionDetailCol <T>,
	HasReservedCols <T> {
}
