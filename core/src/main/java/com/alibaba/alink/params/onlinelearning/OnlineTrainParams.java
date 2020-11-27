package com.alibaba.alink.params.onlinelearning;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.HasVectorSize;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;

public interface OnlineTrainParams<T> extends WithParams <T>,
	HasLabelCol <T>,
	HasVectorColDefaultAsNull <T>,
	HasVectorSize <T>,
	HasFeatureColsDefaultAsNull <T>,
	HasWithIntercept <T>,
	HasTimeInterval_1800 <T> {
}
