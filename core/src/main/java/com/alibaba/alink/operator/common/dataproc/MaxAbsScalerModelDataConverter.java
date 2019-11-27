package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.dataproc.MaxAbsScalerTrainParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class MaxAbsScalerModelDataConverter extends RichModelDataConverter<TableSummary, double[]> {

    public String[] selectedColNames;
    public TypeInformation[] selectedColTypes;

    /**
     * Constructor.
     */
    public MaxAbsScalerModelDataConverter() {
    }

    /**
     * Get the additional column names.
     */
    @Override
    protected String[] initAdditionalColNames() {
        return selectedColNames;
    }

    /**
     * Get the additional column types.
     */
    @Override
    protected TypeInformation[] initAdditionalColTypes() {
        return selectedColTypes;
    }

    /**
     * Serialize the model data to "Tuple3<Params, List<String>, List<Row>>".
     *
     * @param modelData The model data to serialize.
     * @return The serialization result.
     */
    @Override
    public Tuple3<Params, Iterable<String>, Iterable<Row>> serializeModel(TableSummary modelData) {
        String[] colNames = modelData.getColNames();

        double[] maxAbs = new double[colNames.length];
        for (int i = 0; i < colNames.length; i++) {
            //max(|min, max|)
            maxAbs[i] = Math.max(Math.abs(modelData.min(colNames[i])), Math.abs(modelData.max(colNames[i])));
        }

        List<String> data = new ArrayList<>();
        data.add(JsonConverter.toJson(maxAbs));

        return new Tuple3<>(new Params().set(MaxAbsScalerTrainParams.SELECTED_COLS, colNames),
                data, new ArrayList<>());
    }

    /**
     * Deserialize the model data.
     *
     * @param meta         The model meta data.
     * @param data         The model concrete data.
     * @param additionData The additional data.
     * @return The deserialized model data.
     */
    @Override
    public double[] deserializeModel(Params meta, Iterable<String> data, Iterable<Row> additionData) {
        return JsonConverter.fromJson(data.iterator().next(), double[].class);
    }
}
