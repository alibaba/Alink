package com.alibaba.alink.params.outlier;

import com.alibaba.alink.common.annotation.FeatureColsVectorColMutexRule;
import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasTensorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

@ParamSelectColumnSpec(name = "featureCols",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
@ParamSelectColumnSpec(name = "vectorCol",
	allowedTypeCollections = TypeCollections.VECTOR_TYPES)
@ParamSelectColumnSpec(name = "tensorCol",
	allowedTypeCollections = TypeCollections.TENSOR_TYPES)
@FeatureColsVectorColMutexRule
public interface WithMultiVarParams<T> extends
	HasFeatureColsDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T>,
	HasTensorColDefaultAsNull <T> {
}
