package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Parameter of lda predict.
 */
public interface LdaPredictParams<T> extends
	HasSelectedCol <T>,
	RichModelMapperParams <T> {
}
