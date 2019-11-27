package com.alibaba.alink.params.clustering;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.colname.HasVectorCol;

/**
 * Parameter of lda predict.
 */
public interface LdaPredictParams<T> extends
	WithParams<T>,
	HasSelectedCol<T>,
	RichModelMapperParams<T> {
}
