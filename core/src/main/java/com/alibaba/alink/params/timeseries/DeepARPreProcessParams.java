package com.alibaba.alink.params.timeseries;

import com.alibaba.alink.params.nlp.HasWindow;
import com.alibaba.alink.params.shared.colname.HasTimeCol;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasSelectedColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;

public interface DeepARPreProcessParams<T> extends
	HasTimeCol <T>,
	HasSelectedColDefaultAsNull <T>,
	HasVectorColDefaultAsNull <T>,
	HasWindow <T>,
	HasStride <T>,
	HasOutputCols <T> {
}
