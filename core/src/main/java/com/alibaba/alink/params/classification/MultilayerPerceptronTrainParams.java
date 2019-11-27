package com.alibaba.alink.params.classification;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.params.shared.colname.HasFeatureColsDefaultAsNull;
import com.alibaba.alink.params.shared.colname.HasLabelCol;
import com.alibaba.alink.params.shared.colname.HasVectorColDefaultAsNull;
import com.alibaba.alink.params.shared.iter.HasMaxIterDefaultAs100;
import com.alibaba.alink.params.shared.linear.HasEpsilonDv0000001;
import com.alibaba.alink.params.shared.linear.HasL1;
import com.alibaba.alink.params.shared.linear.HasL2;

public interface MultilayerPerceptronTrainParams<T> extends
    HasVectorColDefaultAsNull<T>,
    HasFeatureColsDefaultAsNull<T>,
    HasLabelCol<T>,
    HasMaxIterDefaultAs100<T>,
    HasEpsilonDv0000001<T>,
    HasL1<T>,
    HasL2<T> {

    ParamInfo<int[]> LAYERS = ParamInfoFactory
        .createParamInfo("layers", int[].class)
        .setDescription("Size of each neural network layers.")
        .setRequired()
        .build();
    ParamInfo<Integer> BLOCK_SIZE = ParamInfoFactory
        .createParamInfo("blockSize", Integer.class)
        .setDescription("Size for stacking training samples, the default value is 64.")
        .setHasDefaultValue(64)
        .build();
    ParamInfo<DenseVector> INITIAL_WEIGHTS = ParamInfoFactory
        .createParamInfo("initialWeights", DenseVector.class)
        .setDescription("Initial weights.")
        .setHasDefaultValue(null)
        .build();

    default int[] getLayers() {
        return get(LAYERS);
    }

    default T setLayers(int[] value) {
        return set(LAYERS, value);
    }

    default Integer getBlockSize() {
        return get(BLOCK_SIZE);
    }

    default T setBlockSize(Integer value) {
        return set(BLOCK_SIZE, value);
    }

    default DenseVector getInitialWeights() {
        return get(INITIAL_WEIGHTS);
    }

    default T setInitialWeights(DenseVector value) {
        return set(INITIAL_WEIGHTS, value);
    }
}
