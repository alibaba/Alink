package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.mapper.ModelMapperParams;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.delimiter.HasDelimiterDefaultAsBlank;

public interface AggLookupParams<T> extends
	ModelMapperParams <T>,
	HasReservedColsDefaultAsNull <T>,
	HasDelimiterDefaultAsBlank <T>,
	HasClause <T> {
}
