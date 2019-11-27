package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.pipeline.dataproc.StringIndexer;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Parameters for {@link StringIndexer}.
 */
public interface StringIndexerTrainParams<T> extends WithParams<T>,
    HasSelectedCol<T>,
    HasStringOrderTypeDefaultAsRandom<T> {

    ParamInfo<String> MODEL_NAME = ParamInfoFactory
        .createParamInfo("modelName", String.class)
        .setDescription("Name of the model")
        .build();

    default String getModelName() {
        return get(MODEL_NAME);
    }

    default T setModelName(String colName) {
        return set(MODEL_NAME, colName);
    }
}
