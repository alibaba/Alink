package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasPredictionCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;

public interface TreeModelEncoderParams<T> extends
	ModelMapperParams <T>,
	HasReservedColsDefaultAsNull <T>,
	HasPredictionCol <T> {}
