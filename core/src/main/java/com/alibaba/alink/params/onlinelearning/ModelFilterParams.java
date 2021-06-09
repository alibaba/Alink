package com.alibaba.alink.params.onlinelearning;

import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface ModelFilterParams<T> extends BinaryClassModelFilterParams<T>,
	HasVectorColDefaultAsNull <T> {
}
