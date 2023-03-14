package com.alibaba.alink.params.feature.featuregenerator;

import com.alibaba.alink.params.shared.HasTimeCol_null;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface CommonFeatureParams<T> extends
	HasPrecedingTime <T>,
	HasResultCol<T>,
	HasReservedColsDefaultAsNull<T>,
	HasTimeCol_null<T> {
}
