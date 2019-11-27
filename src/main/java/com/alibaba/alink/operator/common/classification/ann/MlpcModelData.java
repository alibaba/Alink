package com.alibaba.alink.operator.common.classification.ann;

import com.alibaba.alink.common.linalg.DenseVector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.List;

/**
 * The model data for {@link com.alibaba.alink.pipeline.classification.MultilayerPerceptronClassifier}.
 */
public class MlpcModelData {

    public Params meta = new Params();
    public DenseVector weights;
    public TypeInformation labelType;
    public List<Object> labels;

    public MlpcModelData() {
    }

    public MlpcModelData(TypeInformation labelType) {
        this.labelType = labelType;
    }
}
