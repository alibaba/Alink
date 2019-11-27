package com.alibaba.alink.params.onlinelearning;

import com.alibaba.alink.params.shared.HasVectorSize;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;

import org.apache.flink.ml.api.misc.param.WithParams;

public interface FtrlTrainParams<T> extends WithParams<T>,
    HasLabelCol<T>,
    HasVectorColDefaultAsNull<T>,
    HasVectorSize<T>,
    HasFeatureColsDefaultAsNull<T>,
    HasWithIntercept<T>,
    HasTimeInterval_1800<T>,
    HasL1<T>,
    HasL2<T>,
    HasAlpha<T>,
    HasBeta<T> {
}
