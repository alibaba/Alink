package com.alibaba.alink.operator.common.outlier;

import com.alibaba.alink.common.annotation.ParamSelectColumnSpec;
import com.alibaba.alink.common.annotation.TypeCollections;
import com.alibaba.alink.params.outlier.HasDirection;
import com.alibaba.alink.params.shared.colname.HasFeatureColDefaultAsNull;

@ParamSelectColumnSpec(name = "featureCol",
	allowedTypeCollections = TypeCollections.NUMERIC_TYPES)
public interface WithUniVarParams<T> extends
	HasFeatureColDefaultAsNull <T>,
	HasDirection <T> {
}
