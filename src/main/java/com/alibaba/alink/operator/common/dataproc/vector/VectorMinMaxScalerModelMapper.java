package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
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
 * This mapper operates on one row, rescaling each feature
 * to a specific range [min, max). (often [0, 1]).
 */
public class VectorMinMaxScalerModelMapper extends SISOModelMapper {
    private double[] eMins;
    private double[] eMaxs;
    private double min;
    private double max;

    public VectorMinMaxScalerModelMapper(TableSchema modelSchema, TableSchema dataSchema, Params params) {
        super(modelSchema, dataSchema, params.set(VectorSrtPredictorParams.SELECTED_COL,
            RichModelDataConverter.extractSelectedColNames(modelSchema)[0]));
    }

    @Override
    protected TypeInformation initPredResultColType() {
        return VectorTypes.VECTOR;
    }

    /**
     * This function transforms dense vector to normalized format.
     *
     * @param input the input object, maybe sparse vector or dense vector
     * @return the result of prediction
     */
    @Override
    public Object predictResult(Object input) {
        Vector vec = VectorUtil.getVector(input);
        if (null == vec) {
            return null;
        } else if (vec instanceof DenseVector) {
            return predict((DenseVector) vec);
        } else {
            return predict((SparseVector) vec);
        }

    }

    /**
     * Load model from the list of Row type data.
     *
     * @param modelRows the list of Row type data
     */
    @Override
    public void loadModel(List<Row> modelRows) {
        VectorMinMaxScalerModelDataConverter converter = new VectorMinMaxScalerModelDataConverter();
        Tuple4<Double, Double, double[], double[]> tuple4 = converter.load(modelRows);
        min = tuple4.f0;
        max = tuple4.f1;
        eMins = tuple4.f2;
        eMaxs = tuple4.f3;
    }


    /**
     * This function transforms dense vector to normalized format.
     *
     * @param vector the input dense vector
     * @return normalized vector
     */
    private DenseVector predict(DenseVector vector) {
        double[] data = vector.getData();
        for (int i = 0; i < vector.size(); i++) {
            data[i] = ScalerUtil.minMaxScaler(data[i], eMins[i], eMaxs[i], max, min);
        }
        return vector;
    }

    /**
     * This function transforms dense vector to normalized format.
     *
     * @param vector the input sparse vector
     * @return normalized vector
     */
    private Vector predict(SparseVector vector) {
        int n = eMaxs.length;
        DenseVector dv = new DenseVector(n);
        double[] data = dv.getData();
        for (int i = 0; i < data.length; i++) {
            data[i] = ScalerUtil.minMaxScaler(vector.get(i), eMins[i], eMaxs[i], max, min);
        }
        return dv;
    }

}
