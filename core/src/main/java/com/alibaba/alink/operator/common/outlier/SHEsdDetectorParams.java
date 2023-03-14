package com.alibaba.alink.operator.common.outlier;

import com.alibaba.alink.params.outlier.HasDirection;
import com.alibaba.alink.params.outlier.HasMaxOutlierRatio;
import com.alibaba.alink.params.outlier.tsa.HasMaxAnoms;
import com.alibaba.alink.params.outlier.tsa.HasSHESDAlpha;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.colname.HasTimeCol;
import com.alibaba.alink.params.shared.iter.HasMaxIter;
import com.alibaba.alink.params.timeseries.HasFrequency;

public interface SHEsdDetectorParams<T> extends
	HasSelectedCol<T>,
	HasTimeCol<T>,
	HasFrequency <T>,
	HasSHESDAlpha <T>,
	HasDirection <T>,
	HasMaxIter<T> {
}
