package com.alibaba.alink.params.classification;

import com.alibaba.alink.params.mapper.RichModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

/**
 * parameters of linear model mapper process.
 *
 */
public interface LinearModelMapperParams<T> extends
	RichModelMapperParams<T>,
	HasVectorColDefaultAsNull <T> {}
