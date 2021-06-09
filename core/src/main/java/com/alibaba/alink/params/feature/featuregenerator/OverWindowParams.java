package com.alibaba.alink.params.feature.featuregenerator;


import com.alibaba.alink.params.dataproc.HasClause;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.sql.HasOrderBy;

public interface OverWindowParams<T> extends
	HasPartitionColsDefaultAsNull <T>,
	HasOrderBy <T>,
	HasClause <T>,
	HasReservedColsDefaultAsNull <T> {

}
