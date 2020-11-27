package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.shared.colname.HasGroupCol;
import com.alibaba.alink.params.shared.colname.HasOutputCol;

public interface LeaveKObjectOutParams<T> extends
	HasGroupCol <T>,
	HasObjectCol <T>,
	HasFraction <T>,
	HasKDefaultAs10 <T>,
	HasOutputCol <T> {
}
