package com.alibaba.alink.operator.common.feature.AutoCross;

import com.alibaba.alink.operator.common.linear.LinearModelType;

public class DataProfile {
	public int numDistinctLabels;//the label numbers. if linear reg, it is 0; if lr, it is 2.
	public boolean hasIntercept;

	public DataProfile(LinearModelType linearModelType, boolean hasIntercept) {
		this.numDistinctLabels = linearModelType == LinearModelType.LinearReg ? 0 : 2;
		this.hasIntercept = hasIntercept;
	}

}
