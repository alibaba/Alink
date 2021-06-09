package com.alibaba.alink.params.feature;


import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface HashCrossFeatureParams<T> extends
	HasSelectedCols <T>,
	HasNumFeatures<T>,
	HasOutputCol<T>,
	HasReservedColsDefaultAsNull<T> {

}
