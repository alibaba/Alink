package com.alibaba.alink.params.finance;

import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;
import com.alibaba.alink.params.shared.linear.HasPositiveLabelValueStringDefaultAs1;

public interface WoeTrainParams<T> extends
	HasSelectedCols <T>,
	HasPositiveLabelValueStringDefaultAs1 <T>,
	HasLabelCol <T> {
}
