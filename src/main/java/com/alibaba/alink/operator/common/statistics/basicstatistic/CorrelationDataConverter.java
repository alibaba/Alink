package com.alibaba.alink.operator.common.statistics.basicstatistic;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.model.SimpleModelDataConverter;
import com.alibaba.alink.params.statistics.CorrelationParams;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;

import java.util.ArrayList;
import java.util.List;

/**
 *  For correlation model converter, include table and vector.
 */
public class CorrelationDataConverter extends SimpleModelDataConverter<CorrelationResult, CorrelationResult> {

    /**
     * Serialize the model to "Tuple2<Params, List<String>>"
     */
    @Override
    public Tuple2<Params, Iterable<String>> serializeModel(CorrelationResult modelData) {
        List<String> result = new ArrayList<>();

        for (int i = 0; i < modelData.correlation.numRows(); i++) {
            result.add(VectorUtil.toString(new DenseVector(modelData.correlation.getRow(i))));
        }
        if (modelData.colNames != null) {
            return Tuple2.of(new Params()
                    .set(CorrelationParams.SELECTED_COLS, modelData.colNames),
                result);
        } else {
            return Tuple2.of(new Params(), result);
        }
    }

    /**
     * Deserialize the model from "Params meta" and "List<String> data".
     */
    @Override
    public CorrelationResult deserializeModel(Params meta, Iterable<String> data) {


        String[] colNames = null;
        if (meta.contains(CorrelationParams.SELECTED_COLS)) {
            colNames = meta.get(CorrelationParams.SELECTED_COLS);
        }
        DenseMatrix matrix = null;
        int i = 0;
        for (String vecStr : data) {
            DenseVector vec = (DenseVector) VectorUtil.getVector(vecStr);
            if (matrix == null) {
                matrix = new DenseMatrix(vec.size(), vec.size());
            }
            for (int j = 0; j < vec.size(); j++) {
                matrix.set(i, j, vec.get(j));
            }
            i++;
        }

        CorrelationResult modelData = new CorrelationResult(matrix, colNames);
        
        return modelData;
    }
}
