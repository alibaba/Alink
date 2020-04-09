package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.common.utils.JsonConverter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.alink.params.dataproc.ImputerTrainParams.*;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class ImputerModelDataConverter extends RichModelDataConverter<Tuple3<Strategy, TableSummary, String>, Tuple3<Strategy, double[], String>> {
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
    public Tuple3<Params, Iterable<String>, Iterable<Row>> serializeModel(Tuple3<Strategy, TableSummary, String> modelData) {
        Strategy strategy = modelData.f0;
        TableSummary summary = modelData.f1;
        String fillValue = modelData.f2;

        double[] values = null;
        Params meta = new Params()
                .set(STRATEGY, strategy)
                .set(SELECTED_COLS, selectedColNames);
        switch (strategy) {
            case MIN:
                values = new double[selectedColNames.length];
                for (int i = 0; i < selectedColNames.length; i++) {
                    values[i] = summary.min(selectedColNames[i]);
                }
                break;
            case MAX:
                values = new double[selectedColNames.length];
                for (int i = 0; i < selectedColNames.length; i++) {
                    values[i] = summary.max(selectedColNames[i]);
                }
                break;
            case MEAN:
                values = new double[selectedColNames.length];
                for (int i = 0; i < selectedColNames.length; i++) {
                    values[i] = summary.mean(selectedColNames[i]);
                }
                break;
            default:
                meta.set(FILL_VALUE, fillValue);
        }

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
    public Tuple3<Strategy, double[], String> deserializeModel(Params meta, Iterable<String> data, Iterable<Row> additionData) {
        return Tuple3.of(
                meta.get(STRATEGY),
                data.iterator().hasNext() ? JsonConverter.fromJson(data.iterator().next(), double[].class) : null,
                meta.get(FILL_VALUE)
        );
    }
}
