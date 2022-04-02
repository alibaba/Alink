package com.alibaba.alink.params.udf;

import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * @author dota.zk
 * @date 25/06/2019
 */
public interface ScalarFunctionParams<T> extends
    HasResources<T>,
    HasClassName<T>,
    HasResultType<T>,
	HasSelectedCols<T>,
	HasOutputCol<T>,
	HasReservedColsDefaultAsNull<T> {
}
