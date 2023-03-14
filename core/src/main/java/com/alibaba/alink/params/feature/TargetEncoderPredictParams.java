package com.alibaba.alink.params.feature;


import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

public interface TargetEncoderPredictParams <T> extends
	HasSelectedColsDefaultAsNull <T>,
	HasOutputCols <T>,
	HasReservedColsDefaultAsNull <T> {
}
