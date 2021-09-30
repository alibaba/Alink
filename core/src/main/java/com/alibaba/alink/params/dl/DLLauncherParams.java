package com.alibaba.alink.params.dl;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

public interface DLLauncherParams<T> extends WithParams<T>,
    HasNumWorkersDefaultAsNull <T>, HasIntraOpParallelism <T>, HasNumPssDefaultAsNull <T>, HasUserFiles <T> {
    ParamInfo<String> USER_DEFINED_PARAMS = ParamInfoFactory
        .createParamInfo("userDefinedParams", String.class)
        .setDescription("userDefinedParams")
        .setHasDefaultValue("")
        .build();

    default String getUserDefinedParams() {
        return get(USER_DEFINED_PARAMS);
    }

    default T setUserDefinedParams(String value) {
        return set(USER_DEFINED_PARAMS, value);
    }

    ParamInfo<String> PYTHON_ENV = ParamInfoFactory
        .createParamInfo("pythonEnv", String.class)
        .setDescription("python env")
        .setHasDefaultValue("")
        .build();

    default String getPythonEnv() {
        return get(PYTHON_ENV);
    }

    default T setPythonEnv(String value) {
        return set(PYTHON_ENV, value);
    }

    ParamInfo<String> SCRIPT = ParamInfoFactory
        .createParamInfo("script", String.class)
        .setDescription("script")
        .setRequired()
        .build();

    default String getScript() {
        return get(SCRIPT);
    }

    default T setScript(String value) {
        return set(SCRIPT, value);
    }

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

    ParamInfo<String> OUTPUT_SCHEMA_STR = ParamInfoFactory
        .createParamInfo("outputSchemaStr", String.class)
        .setDescription("outputSchemaStr")
        .setRequired()
        .build();

    default String getOutputSchemaStr() {
        return get(OUTPUT_SCHEMA_STR);
    }

    default T setOutputSchemaStr(String value) {
        return set(OUTPUT_SCHEMA_STR, value);
    }
}
