package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.classification.GbdtTrainParams;
import com.alibaba.alink.params.shared.colname.HasGroupColDefaultAsNull;

public interface LambdaMartNdcgParams<T> extends
	GbdtTrainParams <T>,
	HasGroupColDefaultAsNull <T> {
}

