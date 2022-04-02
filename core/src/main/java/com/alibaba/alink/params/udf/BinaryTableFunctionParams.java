package com.alibaba.alink.params.udf;

import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.shared.colname.HasOutputCols;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface BinaryTableFunctionParams<T> extends
	HasClassObject <T>,
	HasClassObjectType <T>,
	HasResultTypes <T>,
	HasSelectedCols <T>,
	HasOutputCols <T>,
	HasPythonVersion <T>,
	HasReservedColsDefaultAsNull <T>,
	HasPythonEnv <T> {
}
