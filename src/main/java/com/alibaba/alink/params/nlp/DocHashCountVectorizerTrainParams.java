package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.feature.HasNumFeatures;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Params for DocHashCountVectorizerTrainBatchOp.
 */
public interface DocHashCountVectorizerTrainParams<T> extends
	HasSelectedCol <T>,
	HasNumFeatures<T>,
	HasMinDF <T>,
	HasFeatureType<T>,
	HasMinTF<T>{
}
