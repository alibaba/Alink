package com.alibaba.alink.params.regression;

import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDv0000001;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;
import com.alibaba.alink.params.shared.linear.HasWithIntercept;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

/**
 * Params for AftRegressionTrainer.
 */
public interface AftRegTrainParams<T> extends
        HasMaxIterDefaultAs100<T>,
        HasEpsilonDv0000001<T>,
        HasWithIntercept<T>,
        HasLabelCol<T>,
        HasVectorColDefaultAsNull<T>,
		HasFeatureColsDefaultAsNull<T>,
        HasL1<T>,
        HasL2<T> {

    ParamInfo<String> CENSOR_COL = ParamInfoFactory
            .createParamInfo("censorCol", String.class)
            .setDescription(
                    "The value of this column could only be 0 or 1. If the value is 1, it means the event has occurred.")
            .setRequired()
            .setAlias(new String[]{"censorColName"})
            .build();

    default String getCensorCol() {
        return get(CENSOR_COL);
    }

    default T setCensorCol(String value) {
        return set(CENSOR_COL, value);
    }
}
