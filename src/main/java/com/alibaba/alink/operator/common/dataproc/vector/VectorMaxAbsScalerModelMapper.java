package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.operator.common.dataproc.ScalerUtil;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.common.mapper.SISOModelMapper;
import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.common.VectorTypes;
import com.alibaba.alink.params.dataproc.vector.VectorSrtPredictorParams;

import java.util.List;

/**
 * This mapper operates on one row,rescaling each feature to range
 * [-1, 1] by dividing through the maximum absolute value in each feature.
 */
public class VectorMaxAbsScalerModelMapper extends SISOModelMapper {
    private double[] maxAbs;

    public VectorMaxAbsScalerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params.set(VectorSrtPredictorParams.SELECTED_COL,
            RichModelDataConverter.extractSelectedColNames(modelSchema)[0]));
    }

    @Override
    protected TypeInformation initPredResultColType() {
        return VectorTypes.VECTOR;
    }

    /**
     * This function process the values of input vector with dividing maxAbs value of the vectors.
     *
     * @param input the input object, maybe sparse vector or dense vector
     * @return the result of prediction
     */
    @Override
    protected Object predictResult(Object input) {
        Vector vec = VectorUtil.getVector(input);
        if (null == vec) {
            return null;
        } else if (vec instanceof DenseVector) {
            return predict((DenseVector)vec);
        } else {
            return predict((SparseVector)vec);
        }

    }

    /**
     * Load model from the list of Row type data.
     *
     * @param modelRows the list of Row type data
     */
    @Override
    public void loadModel(List<Row> modelRows) {
        VectorMaxAbsScalerModelDataConverter converter = new VectorMaxAbsScalerModelDataConverter();
        maxAbs = converter.load(modelRows);
    }

    /**
     * This function transforms one DenseVector data to normalized format.
     *
     * @param vector the input dense vector
     * @return normalized vector
     */
    private DenseVector predict(DenseVector vector) {
        double[] data = vector.getData();
        for (int i = 0; i < vector.size(); i++) {
            data[i] = ScalerUtil.maxAbsScaler(maxAbs[i], data[i]);
        }
        return vector;
    }

    /**
     * This function transforms one SparseVector data to normalized format.
     *
     * @param vector the input sparse vector
     * @return normalized vector
     */
    private SparseVector predict(SparseVector vector) {
        for (int i = 0; i < vector.numberOfValues(); i++) {
            int idx = vector.getIndices()[i];
            vector.getValues()[i] = ScalerUtil.maxAbsScaler(maxAbs[idx], vector.getValues()[i]);
        }
        return vector;
    }
}
