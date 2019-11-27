package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.dataproc.MaxAbsScalerTrainParams;
import com.alibaba.alink.params.dataproc.MinMaxScalerTrainParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This converter can help serialize and deserialize the model data.
 */
public class MinMaxScalerModelDataConverter extends RichModelDataConverter<
    Tuple3<Double, Double, TableSummary>,
    Tuple4<Double, Double, double[], double[]>> {

    public String[] selectedColNames;
    public TypeInformation[] selectedColTypes;

    /**
     * Constructor.
     */
    public MinMaxScalerModelDataConverter() {
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
    public Tuple3<Params, Iterable<String>, Iterable<Row>> serializeModel(
        Tuple3<Double, Double, TableSummary> modelData) {
        double min = modelData.f0;
        double max = modelData.f1;
        TableSummary summary = modelData.f2;

        String[] colNames = summary.getColNames();

        double[] eMaxs = new double[colNames.length];
        double[] eMins = new double[colNames.length];

        for (int i = 0; i < colNames.length; i++) {
            eMaxs[i] = summary.max(colNames[i]);
            eMins[i] = summary.min(colNames[i]);
        }

        List<String> data = new ArrayList<>();
        data.add(JsonConverter.toJson(eMins));
        data.add(JsonConverter.toJson(eMaxs));

        Params meta = new Params()
            .set(MaxAbsScalerTrainParams.SELECTED_COLS, colNames)
            .set(MinMaxScalerTrainParams.MIN, min)
            .set(MinMaxScalerTrainParams.MAX, max);

        return new Tuple3<>(meta, data, new ArrayList<>());
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
    public Tuple4<Double, Double, double[], double[]> deserializeModel(
        Params meta, Iterable<String> data, Iterable<Row> additionData) {
        Iterator<String> iter = data.iterator();
        return new Tuple4<>(meta.get(MinMaxScalerTrainParams.MIN),
            meta.get(MinMaxScalerTrainParams.MAX),
            JsonConverter.fromJson(iter.next(), double[].class),
            JsonConverter.fromJson(iter.next(), double[].class)
        );
    }

}

