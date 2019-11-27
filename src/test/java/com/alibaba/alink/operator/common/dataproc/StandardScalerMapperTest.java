package com.alibaba.alink.operator.common.dataproc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import org.apache.flink.ml.api.misc.param.Params;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StandardScalerMapperTest {

    TableSchema modelSchema = new TableSchema(new String[]{"model_id", "model_info", "f_double", "f_long", "f_int"},
        new TypeInformation[]{Types.LONG, Types.STRING, Types.DOUBLE, Types.LONG, Types.INT});

    @Test
    public void test() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0L, "{\"withMean\":\"true\",\"withStd\":\"true\"}", null, null, null),
            Row.of(1048576L, "[1.0,1.0,0.2]", null, null, null),
            Row.of(2097152L, "[1.0,1.0,1.0]", null, null, null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"f_string", "f_long", "f_int", "f_double", "f_boolean"},
            new TypeInformation<?>[]{Types.STRING, Types.LONG, Types.INT, Types.DOUBLE, Types.BOOLEAN}
        );
        Params params = new Params();

        StandardScalerModelMapper mapper = new StandardScalerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals((double) mapper.map(Row.of("a", 1L, 1, 2.0, true)).getField(1), 0.0, 10e-4);
        assertEquals((double) mapper.map(Row.of("a", 1L, 1, 2.0, true)).getField(2), 0.8, 10e-4);
        assertEquals((double) mapper.map(Row.of("a", 1L, 1, 2.0, true)).getField(3), 1.0, 10e-4);

    }

}