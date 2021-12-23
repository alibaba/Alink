package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.io.HasSchemaStr;
import com.alibaba.alink.params.shared.HasHandleInvalid;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;

/**
 * Params for RecommTableToKv.
 */
public interface FlattenMTableParams<T> extends
	HasSelectedCol <T>,
	HasSchemaStr<T>,
	HasReservedColsDefaultAsNull <T>,
	HasHandleInvalid<T> {
}
