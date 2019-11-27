package com.alibaba.alink.operator.common.dataproc.vector;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.dataproc.vector.VectorMinMaxScalerTrainParams;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class VectorMinMaxScalerModelDataConverter extends
    RichModelDataConverter<Tuple3<Double, Double, BaseVectorSummary>, Tuple4<Double, Double, double[], double[]>> {

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
    public Tuple3<Params, Iterable<String>, Iterable<Row>> serializeModel(Tuple3<Double, Double, BaseVectorSummary> modelData) {
        double min = modelData.f0;
        double max = modelData.f1;

        BaseVectorSummary summary = modelData.f2;

        double[] eMins;
        double[] eMaxs;
        if (summary.min() instanceof DenseVector) {
            eMins = ((DenseVector) summary.min()).getData();
        } else {
            eMins = ((SparseVector) summary.min()).toDenseVector().getData();
        }
        if (summary.max() instanceof DenseVector) {
            eMaxs = ((DenseVector) summary.max()).getData();
        } else {
            eMaxs = ((SparseVector) summary.min()).toDenseVector().getData();
        }

        List<String> data = new ArrayList<>();
        data.add(JsonConverter.toJson(eMins));
        data.add(JsonConverter.toJson(eMaxs));

        Params meta = new Params()
            .set(VectorMinMaxScalerTrainParams.MIN, min)
            .set(VectorMinMaxScalerTrainParams.MAX, max)
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
    public Tuple4<Double, Double, double[], double[]> deserializeModel(Params meta, Iterable<String> data, Iterable<Row> additionData) {
        double min = meta.get(VectorMinMaxScalerTrainParams.MIN);
        double max = meta.get(VectorMinMaxScalerTrainParams.MAX);

        double[] eMins = JsonConverter.fromJson(data.iterator().next(), double[].class);
        double[] eMaxs = JsonConverter.fromJson(data.iterator().next(), double[].class);

        return Tuple4.of(min, max, eMins, eMaxs);
    }

}
