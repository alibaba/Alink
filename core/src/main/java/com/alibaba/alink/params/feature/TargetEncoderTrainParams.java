package com.alibaba.alink.params.feature;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueString;

public interface TargetEncoderTrainParams <T> extends
	HasLabelCol <T>,
	HasPositiveLabelValueString <T>,
	HasSelectedColsDefaultAsNull <T> {}
