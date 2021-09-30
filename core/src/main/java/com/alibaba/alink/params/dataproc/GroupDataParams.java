package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

public interface GroupDataParams<T> extends
	HasGroupCols <T>,
	HasSelectedColsDefaultAsNull <T>,
	HasOutputCol <T> {
}
