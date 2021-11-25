package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

import com.alibaba.alink.params.tensorflow.savedmodel.HasOutputSchemaStr;

public interface DLLauncherParams<T> extends WithParams<T>,
    HasNumWorkersDefaultAsNull <T>, HasIntraOpParallelism <T>, HasNumPssDefaultAsNull <T>, HasUserFiles <T>,
	HasPythonEnv <T>, HasUserParams <T>, HasMainScriptFile <T>, HasOutputSchemaStr <T> {

    ParamInfo<String> ENTRY_FUNC = ParamInfoFactory
        .createParamInfo("entryFunc", String.class)
        .setDescription("entry func")
        .setRequired()
        .build();

    default String getEntryFunc() {
        return get(ENTRY_FUNC);
    }

    default T setEntryFunc(String value) {
        return set(ENTRY_FUNC, value);
    }
}
