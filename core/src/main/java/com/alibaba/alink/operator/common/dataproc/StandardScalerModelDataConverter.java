package com.alibaba.alink.operator.common.dataproc;

import com.alibaba.alink.common.model.RichModelDataConverter;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.params.dataproc.StandardTrainParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StandardScalerModelDataConverter extends RichModelDataConverter<
    Tuple3<Boolean, Boolean, TableSummary>,
    Tuple2<double[], double[]>> {

    public String[] selectedColNames;
    public TypeInformation[] selectedColTypes;

    public StandardScalerModelDataConverter() {
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
    public Tuple3<Params, Iterable<String>, Iterable<Row>> serializeModel(Tuple3<Boolean, Boolean, TableSummary> modelData) {
        Boolean withMean = modelData.f0;
        Boolean withStandarDeviation = modelData.f1;
        TableSummary summary = modelData.f2;

        String[] colNames = summary.getColNames();

        double[] means = new double[colNames.length];
        double[] stdDevs = new double[colNames.length];

        for (int i = 0; i < colNames.length; i++) {
            means[i] = summary.mean(colNames[i]);
            stdDevs[i] = summary.standardDeviation(colNames[i]);
        }

        for (int i = 0; i < colNames.length; i++) {
            if (!withMean) {
                means[i] = 0;
            }
            if (!withStandarDeviation) {
                stdDevs[i] = 1;
            }
        }

        Params meta = new Params()
            .set(StandardTrainParams.WITH_MEAN, withMean)
            .set(StandardTrainParams.WITH_STD, withStandarDeviation);

        List<String> data = new ArrayList<>();
        data.add(JsonConverter.toJson(means));
        data.add(JsonConverter.toJson(stdDevs));

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
    public Tuple2<double[], double[]> deserializeModel(Params meta, Iterable<String> data, Iterable<Row> additionData) {
        Iterator<String> iter = data.iterator();
        return new Tuple2<>(
            JsonConverter.fromJson(iter.next(), double[].class),
            JsonConverter.fromJson(iter.next(), double[].class)
        );
    }

}

