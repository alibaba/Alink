package com.alibaba.alink.params.feature.featuregenerator;

import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface BaseOverWindowParams<T> extends
	HasPartitionColsDefaultAsNull <T>,
	BaseWindowParams <T>,
	HasReservedColsDefaultAsNull <T> {
}
