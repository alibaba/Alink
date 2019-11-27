package com.alibaba.alink.operator.common.dataproc.vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerTrainParams;
import com.alibaba.alink.params.dataproc.vector.VectorStandardTrainParams;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class VectorStandardScalerModelDataConverter extends
    RichModelDataConverter<Tuple3<Boolean, Boolean, BaseVectorSummary>, Tuple4<Boolean, Boolean, double[], double[]>> {

    public String vectorColName;


    /**
     * Get the additional column names.
     */
    @Override
    protected String[] initAdditionalColNames() {
        return new String[]{vectorColName};
    }

    /**
     * Get the additional column types.
     */
    @Override
    protected TypeInformation[] initAdditionalColTypes() {
        return new TypeInformation[]{Types.STRING};
    }

    /**
     * Serialize the model data to "Tuple3<Params, List<String>, List<Row>>".
     *
     * @param modelData The model data to serialize.
     * @return The serialization result.
     */
    public Tuple3<Params, Iterable<String>, Iterable<Row>> serializeModel(Tuple3<Boolean, Boolean, BaseVectorSummary> modelData) {
        Boolean withMean = modelData.f0;
        Boolean withStd = modelData.f1;
        BaseVectorSummary summary = modelData.f2;

        double[] means;
        double[] stdDeviations;

        int n = summary.vectorSize();
        if (withMean) {
            if (summary.mean() instanceof DenseVector) {
                means = ((DenseVector) summary.mean()).getData();
            } else {
                means = ((SparseVector) summary.mean()).toDenseVector().getData();
            }
        } else {
            means = new double[n];
        }
        if (withStd) {
            if (summary.standardDeviation() instanceof DenseVector) {
                stdDeviations = ((DenseVector) summary.standardDeviation()).getData();
            } else {
                stdDeviations = ((SparseVector) summary.standardDeviation()).toDenseVector().getData();
            }
        } else {
            stdDeviations = new double[n];
            Arrays.fill(stdDeviations, 1);
        }

        List<String> data = new ArrayList<>();
        data.add(JsonConverter.toJson(means));
        data.add(JsonConverter.toJson(stdDeviations));

        Params meta = new Params()
            .set(VectorStandardTrainParams.WITH_MEAN, withMean)
            .set(VectorStandardTrainParams.WITH_STD, withStd)
            .set(VectorMinMaxScalerTrainParams.SELECTED_COL, vectorColName);

        return Tuple3.of(meta, data, new ArrayList<>());
    }

    /**
     * Deserialize the model data.
     *
     * @param meta         The model meta data.
     * @param data         The model concrete data.
     * @param additionData The additional data.
     * @return The model data used by mapper.
     */
    @Override
    public Tuple4<Boolean, Boolean, double[], double[]> deserializeModel(Params meta, Iterable<String> data, Iterable<Row> additionData) {
        double[] means = JsonConverter.fromJson(data.iterator().next(), double[].class);
        double[] stdDevs = JsonConverter.fromJson(data.iterator().next(), double[].class);

        Boolean withMean = meta.get(VectorStandardTrainParams.WITH_MEAN);
        Boolean withStd = meta.get(VectorStandardTrainParams.WITH_STD);

        return Tuple4.of(withMean, withStd, means, stdDevs);
    }

}
