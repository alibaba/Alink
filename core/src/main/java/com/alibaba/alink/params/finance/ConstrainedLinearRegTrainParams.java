package com.alibaba.alink.params.finance;

import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDefaultAs0000001;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.HasStandardization;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;

public interface ConstrainedLinearRegTrainParams<T>
	extends ConstrainedLinearModelParams <T>,
	HasWithIntercept <T>,
	HasMaxIterDefaultAs100 <T>,
	HasEpsilonDefaultAs0000001 <T>,
	HasFeatureColsDefaultAsNull <T>,
	HasLabelCol <T>,
	HasWeightColDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T>,
	HasStandardization <T>,
	HasL1 <T>,
	HasL2 <T>,
	HasConstrainedOptimizationMethod <T> {

}
