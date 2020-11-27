package com.alibaba.alink.params.nlp;

import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;

public interface KeywordsExtractionParams<T> extends
	HasSelectedCol <T>,
	HasTopNDv10 <T>,
	HasWindowSize_2 <T>,
	HasDampingFactor <T>,
	HasMaxIterDefaultAs100 <T>,
	HasOutputColDefaultAsNull <T>,
	HasEpsilon <T>,
	HasMethod <T> {
}
