package com.alibaba.alink.params.dl;

import com.alibaba.alink.params.shared.colname.HasSelectedColsDefaultAsNull;

public interface BasePythonParams<T> extends HasSelectedColsDefaultAsNull <T>,
	HasPythonEnv <T>,
	HasIntraOpParallelism <T>, HasUserFiles <T>,
	HasMainScriptFile <T>, HasUserParams <T> {
}
