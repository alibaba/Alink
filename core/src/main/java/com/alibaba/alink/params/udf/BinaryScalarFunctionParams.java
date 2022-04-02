package com.alibaba.alink.params.udf;

import com.alibaba.alink.params.dl.HasPythonEnv;
import com.alibaba.alink.params.shared.colname.HasOutputCol;
import com.alibaba.alink.params.shared.colname.HasReservedColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasSelectedCols;

public interface BinaryScalarFunctionParams<T> extends
	HasClassObject <T>,
	HasClassObjectType <T>,
	HasResultType <T>,
	HasSelectedCols <T>,
	HasOutputCol <T>,
	HasPythonVersion <T>,
	HasReservedColsDefaultAsNull <T>,
	HasPythonEnv <T> {
}
