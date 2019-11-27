package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.dataproc.ImputerTrainParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class ImputerModelDataConverter extends RichModelDataConverter<Tuple2<String, TableSummary>, Tuple2<String, double[]>> {
    public String[] selectedColNames;
    public TypeInformation[] selectedColTypes;

    /**
     * Constructor.
     */
    public ImputerModelDataConverter() {
    }

    @Override
    protected String[] initAdditionalColNames() {
        return selectedColNames;
    }

    @Override
    protected TypeInformation[] initAdditionalColTypes() {
        return selectedColTypes;
    }

    /**
     * Serialize the model to "Tuple3<Params, List<String>, List<Row>>"
     *
     * @param modelData The model data to serialize.
     * @return The serialization result.
     */
    @Override
    public Tuple3<Params, Iterable<String>, Iterable<Row>> serializeModel(Tuple2<String, TableSummary> modelData) {
        String strategy = modelData.f0.trim().toUpperCase();
        TableSummary summary = modelData.f1;

        double[] values = null;

        switch (strategy.trim().toUpperCase()) {
            case "MIN":
                values = new double[selectedColNames.length];
                for (int i = 0; i < selectedColNames.length; i++) {
                    values[i] = summary.min(selectedColNames[i]);
                }
                break;
            case "MAX":
                values = new double[selectedColNames.length];
                for (int i = 0; i < selectedColNames.length; i++) {
                    values[i] = summary.max(selectedColNames[i]);
                }
                break;
            case "MEAN":
                values = new double[selectedColNames.length];
                for (int i = 0; i < selectedColNames.length; i++) {
                    values[i] = summary.mean(selectedColNames[i]);
                }
                break;
            default:
        }
        Params meta = new Params()
                .set(ImputerTrainParams.STRATEGY, strategy)
                .set(ImputerTrainParams.SELECTED_COLS, selectedColNames);
        List<String> data = new ArrayList<>();
        data.add(JsonConverter.toJson(values));

        return Tuple3.of(meta, data, new ArrayList<>());
    }

    /**
     * Deserialize the model from "Params meta", "List<String> data" and "List<Row>>".
     *
     * @param meta         The model meta data.
     * @param data         The model concrete data.
     * @param additionData The additional data.
     * @return The deserialized model data.
     */
    @Override
    public Tuple2<String, double[]> deserializeModel(Params meta, Iterable<String> data, Iterable<Row> additionData) {
        return Tuple2.of(
                meta.get(ImputerTrainParams.STRATEGY),
                data.iterator().hasNext() ? JsonConverter.fromJson(data.iterator().next(), double[].class) : null
        );
    }
}
