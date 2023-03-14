package com.alibaba.alink.params.finance;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasSelectedColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;

public interface StepwiseSelectorParams<T> extends
	HasSelectedColDefaultAsNull <T>,
	HasSelectedColsDefaultAsNull <T>,
	HasLabelCol <T>,
	HasL1 <T>,
	HasL2 <T>,
	HasAlphaEntry <T>,
	HasAlphaStay <T>,
	HasForceSelectedCols <T> {
}
