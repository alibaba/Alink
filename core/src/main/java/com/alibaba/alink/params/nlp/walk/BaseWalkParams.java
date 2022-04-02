package com.alibaba.alink.params.nlp.walk;

import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.shared.colname.HasWeightColDefaultAsNull;
import com.alibaba.alink.params.shared.delimiter.HasDelimiterDefaultAsBlank;

public interface BaseWalkParams<T> extends WithParams <T>,
	HasSourceCol <T>,
	HasTargetCol <T>,
		HasDelimiterDefaultAsBlank<T>,
	HasWeightColDefaultAsNull <T>,
	HasWalkLength <T>,
	HasWalkNum <T>,
	HasIsToUndigraph <T> {
}
