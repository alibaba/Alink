package com.alibaba.alink.operator.common.clustering.kmeans;

import com.alibaba.alink.params.clustering.KMeansPredictParams;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for KMeansModelMapper.
 */
public class KMeansOldModelMapper1Test {
    private Row[] rows = new Row[] {
        Row.of(0L, "{\"vectorCol\":\"\\\"Y\\\"\",\"latitudeCol\":null,\"longitudeCol\":null,"
            + "\"distanceType\":\"\\\"EUCLIDEAN\\\"\",\"k\":\"2\",\"modelSchema\":\"\\\"model_id bigint,model_info "
            + "string\\\"\",\"isNewFormat\":\"true\",\"vectorSize\":\"3\"}"),
        Row.of(1048576L, "{\"center\":\"{\\\"data\\\":[9.1,9.1,9.1]}\",\"clusterId\":0,\"weight\":3.0}"),
        Row.of(2097152L, "{\"center\":\"{\\\"data\\\":[0.1,0.1,0.1]}\",\"clusterId\":1,\"weight\":3.0}")
    };

    private List<Row> model = Arrays.asList(rows);
    private TableSchema modelSchema = new KMeansModelDataConverter().getModelSchema();

    @Test
    public void testDefault() throws Exception {
        TableSchema dataSchema = new TableSchema(
            new String[] {"Y"}, new TypeInformation<?>[] {Types.STRING}
        );
        Params params = new Params()
            .set(KMeansPredictParams.PREDICTION_COL, "pred");

        KMeansModelMapper mapper = new KMeansModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("0 0 0")).getField(1), 1L);
        assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"Y", "pred"},
            new TypeInformation<?>[] {Types.STRING, Types.LONG}));
    }

    @Test
    public void testHaversineDistance() throws Exception {
        Row[] rows = new Row[] {
            Row.of(0L, "{\"vectorCol\":null,\"latitudeCol\":\"\\\"f1\\\"\",\"longitudeCol\":\"\\\"f0\\\"\","
                + "\"distanceType\":\"\\\"HAVERSINE\\\"\",\"k\":\"2\",\"modelSchema\":\"\\\"model_id bigint,"
                + "model_info string\\\"\",\"isNewFormat\":\"true\",\"vectorSize\":\"2\"}"),
            Row.of(1048576L, "{\"center\":\"{\\\"data\\\":[8.33,9.0]}\",\"clusterId\":0,\"weight\":3.0}"),
            Row.of(2097152L, "{\"center\":\"{\\\"data\\\":[1.0,1.33]}\",\"clusterId\":1,\"weight\":3.0}")
        };

        List<Row> model = Arrays.asList(rows);
        TableSchema modelSchema = new KMeansModelDataConverter().getModelSchema();

        TableSchema dataSchema = new TableSchema(
            new String[] {"f0", "f1"}, new TypeInformation<?>[] {Types.DOUBLE, Types.DOUBLE}
        );
        Params params = new Params()
            .set(KMeansPredictParams.PREDICTION_COL, "pred");

        KMeansModelMapper mapper = new KMeansModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of(0, 0)).getField(2), 1L);
        assertEquals(mapper.getOutputSchema(), new TableSchema(new String[] {"f0", "f1", "pred"},
            new TypeInformation<?>[] {Types.DOUBLE, Types.DOUBLE, Types.LONG}));
    }
}
