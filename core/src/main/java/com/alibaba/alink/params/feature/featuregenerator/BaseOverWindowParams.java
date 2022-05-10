package com.alibaba.alink.params.feature.featuregenerator;

import com.alibaba.alink.params.shared.colname.HasGroupColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface BaseOverWindowParams<T> extends
	HasGroupColsDefaultAsNull <T>,
	BaseWindowParams <T>,
	HasReservedColsDefaultAsNull <T> {
}
