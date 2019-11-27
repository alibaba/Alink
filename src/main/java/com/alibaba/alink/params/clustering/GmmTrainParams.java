package com.alibaba.alink.params.clustering;

import com.alibaba.alink.params.shared.clustering.HasKDefaultAs2;
import com.alibaba.alink.params.shared.colname.HasVectorCol;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * Parameters for Gaussian Mixture Model training.
 *
 * @param <T> The class that implement this interface.
 */
public interface GmmTrainParams<T> extends WithParams<T>,
    HasVectorCol<T>,
    HasKDefaultAs2<T>,
    HasMaxIterDefaultAs100<T> {

    ParamInfo<Double> TOL = ParamInfoFactory
        .createParamInfo("tol", Double.class)
        .setDescription("Iteration tolerance.")
        .setHasDefaultValue(0.01)
        .build();

    default Double getTol() {
        return get(TOL);
    }

    default T setTol(Double value) {
        return set(TOL, value);
    }
}
