package com.alibaba.alink.operator.common.dataproc.vector;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.common.utils.JsonConverter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class VectorMaxAbsScalerModelDataConverter extends
    RichModelDataConverter<BaseVectorSummary, double[]> {

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
    public Tuple3<Params, Iterable<String>, Iterable<Row>> serializeModel(BaseVectorSummary modelData) {

        double[] maxData;
        double[] minData;
        if (modelData.max() instanceof DenseVector) {
            maxData = ((DenseVector) modelData.max()).getData();
        } else {
            maxData = ((SparseVector) modelData.max()).toDenseVector().getData();
        }
        if (modelData.min() instanceof DenseVector) {
            minData = ((DenseVector) modelData.min()).getData();
        } else {
            minData = ((SparseVector) modelData.min()).toDenseVector().getData();
        }

        double[] maxAbs = new double[maxData.length];
        for (int i = 0; i < maxAbs.length; i++) {
            maxAbs[i] = Math.max(Math.abs(minData[i]), Math.abs(maxData[i]));
        }

        List<String> data = new ArrayList<>();
        data.add(JsonConverter.toJson(maxAbs));

        return Tuple3.of(new Params(), data, new ArrayList<>());
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
    public double[] deserializeModel(Params meta, Iterable<String> data, Iterable<Row> additionData) {
        return JsonConverter.fromJson(data.iterator().next(), double[].class);
    }

}
