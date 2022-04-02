package com.alibaba.alink.params.udf;

import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

/**
 * @author dota.zk
 * @date 25/06/2019
 */
public interface TableFunctionParams<T> extends
    HasResources<T>,
    HasClassName<T>,
    HasResultTypes<T>,
	HasSelectedCols<T>,
	HasOutputCols<T>,
	HasReservedColsDefaultAsNull<T> {
}
