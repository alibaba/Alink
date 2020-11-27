package com.alibaba.alink.params.recommendation;

import com.alibaba.alink.params.shared.colname.HasOutputColTypesDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Params for RecommTableToKv.
 */
public interface FlattenKObjectParams<T> extends
	HasSelectedCol <T>,
	HasOutputCols <T>,
	HasOutputColTypesDefaultAsNull <T>,
	HasReservedColsDefaultAsNull <T> {
}
