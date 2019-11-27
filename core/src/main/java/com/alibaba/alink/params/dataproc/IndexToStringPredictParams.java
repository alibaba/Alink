package com.alibaba.alink.params.dataproc;

import com.alibaba.alink.params.shared.colname.HasOutputColDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasReservedCols;
import com.alibaba.alink.params.shared.colname.HasSelectedCol;
import com.alibaba.alink.pipeline.dataproc.IndexToString;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Parameters for {@link IndexToString}.
 */
public interface IndexToStringPredictParams<T> extends
    HasSelectedCol<T>,
    HasReservedCols<T>,
    HasOutputColDefaultAsNull<T> {

    ParamInfo<String> MODEL_NAME = ParamInfoFactory
        .createParamInfo("modelName", String.class)
        .setDescription("Name of the model")
        .setRequired()
        .build();

    default String getModelName() {
        return get(MODEL_NAME);
    }

    default T setModelName(String colName) {
        return set(MODEL_NAME, colName);
    }
}
