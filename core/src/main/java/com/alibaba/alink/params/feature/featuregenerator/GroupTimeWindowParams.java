package com.alibaba.alink.params.feature.featuregenerator;

import com.alibaba.alink.params.shared.colname.HasGroupColsDefaultAsNull;

public interface GroupTimeWindowParams<T>
	extends BaseWindowParams <T>,
	HasGroupColsDefaultAsNull <T> {

}
