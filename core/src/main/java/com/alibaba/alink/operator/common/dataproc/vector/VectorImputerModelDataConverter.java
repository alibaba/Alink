package com.alibaba.alink.operator.common.dataproc.vector;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.dataproc.vector.VectorImputerTrainParams;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class VectorImputerModelDataConverter extends
    RichModelDataConverter<Tuple2<String, BaseVectorSummary>, Tuple2<String, double[]>> {

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
    public Tuple3<Params, Iterable<String>, Iterable<Row>> serializeModel(Tuple2<String, BaseVectorSummary> modelData) {
        String strategy = modelData.f0;
        BaseVectorSummary summary = modelData.f1;
        double[] values = null;
        if (summary != null) {
            switch (Strategy.valueOf(strategy.trim().toUpperCase())) {
                case MIN:
                    if (summary.min() instanceof DenseVector) {
                        values = ((DenseVector) summary.min()).getData();
                    } else {
                        values = ((SparseVector) summary.min()).toDenseVector().getData();
                    }
                    break;
                case MAX:
                    if (summary.max() instanceof DenseVector) {
                        values = ((DenseVector) summary.max()).getData();
                    } else {
                        values = ((SparseVector) summary.max()).toDenseVector().getData();
                    }
                    break;
                case MEAN:
                    if (summary.mean() instanceof DenseVector) {
                        values = ((DenseVector) summary.mean()).getData();
                    } else {
                        values = ((SparseVector) summary.mean()).getValues();
                    }
                    break;
                default:
            }
        }

        Params meta = new Params()
            .set(VectorImputerTrainParams.SELECTED_COL, vectorColName)
            .set(VectorImputerTrainParams.STRATEGY, strategy);

        List<String> data = new ArrayList<>();
        data.add(JsonConverter.toJson(values));

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
    public Tuple2<String, double[]> deserializeModel(Params meta, Iterable<String> data, Iterable<Row> additionData) {
        String strategy = meta.get(VectorImputerTrainParams.STRATEGY);
        double[] values = null;
        if(data.iterator().hasNext()) {
            values = JsonConverter.fromJson(data.iterator().next(), double[].class);
        }
        return Tuple2.of(strategy, values);
    }

    private enum Strategy {
        /**
         * It will replace missing value with min of the column.
         */
        MIN,
        /**
         * It will replace missing value with max of the column.
         */
        MAX,
        /**
         * It will replace missing value with mean of the column.
         */
        MEAN
    }
}
