package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

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
 * This mapper transforms a dataset, normalizing each feature to have unit standard deviation and/or zero mean.
 * If withMean is false, set mean as 0; if withStd is false, set std as 1.
 */
public class VectorStandardScalerModelMapper extends SISOModelMapper {
    private double[] means;
    private double[] stdDeviations;
    private Boolean withMean;

    /**
     * Constructor.
     * @param modelSchema the model schema.
     * @param dataSchema  the data schema.
     * @param params      the params.
     */
    public VectorStandardScalerModelMapper(TableSchema modelSchema,
                                           TableSchema dataSchema,
                                           Params params) {
        super(modelSchema, dataSchema, params.set(VectorSrtPredictorParams.SELECTED_COL,
                RichModelDataConverter.extractSelectedColNames(modelSchema)[0]));
    }

    @Override
    protected TypeInformation initPredResultColType() {
        return VectorTypes.VECTOR;
    }

    /**
     * This function transforms dense vector to standard format.
     *
     * @param input the input object, maybe sparse vector or dense vector
     * @return the result of prediction
     */
    @Override
    protected Object predictResult(Object input) {
        if (null == input) {
            return null;
        }
        Vector vec = VectorUtil.getVector(input);
        if (vec instanceof DenseVector) {
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
        VectorStandardScalerModelDataConverter converter = new VectorStandardScalerModelDataConverter();
        Tuple4<Boolean, Boolean, double[], double[]> tuple4 = converter.load(modelRows);
        this.withMean = tuple4.f0;
        this.means = tuple4.f2;
        this.stdDeviations = tuple4.f3;
    }


    /**
     * This function performs standardized operation on the input dense vector and finally return it.
     *
     * @param vector the input dense vector
     * @return standard vector
     */
    private DenseVector predict(DenseVector vector) {
        double[] data = vector.getData();
        for (int i = 0; i < vector.size(); ++i) {
            if (stdDeviations[i] != 0) {
                data[i] = (data[i] - means[i]) / stdDeviations[i];
            } else {
                data[i] = 0;
            }
        }
        return vector;
    }

    /**
     * This function performs standardized operation on the input sparse vector. If the withMean parameter is true,
     * then modifies the input sparse vector to a dense one and then return it; if the withMean parameter is false,
     * then operates on the input vector and finally return it.
     *
     * @param vector the input sparse vector
     * @return standard vector
     */
    private Vector predict(SparseVector vector) {
        int[] indices = vector.getIndices();
        double[] values = vector.getValues();
        if (withMean) {
            int n = means.length;
            DenseVector dv = new DenseVector(n);
            double[] data = dv.getData();
            int point = 0;
            for (int i = 0; i < data.length; ++i) {
                if (point == indices.length) {
                    break;
                }
                if (stdDeviations[i] != 0) {
                    if (indices[point] == i) {
                        data[i] = (values[point++] - means[i]) / stdDeviations[i];
                    } else {
                        data[i] = -means[i] / stdDeviations[i];
                    }
                } else if (indices[point] == i) {
                    point++;
                }
            }
            return dv;
        } else {
            for (int i = 0; i < vector.numberOfValues(); ++i) {
                int idx = indices[i];
                if (stdDeviations[idx] != 0) {
                    values[i] = values[i] / stdDeviations[idx];
                } else {
                    values[i] = 0;
                }
            }
            return vector;
        }
    }

}
