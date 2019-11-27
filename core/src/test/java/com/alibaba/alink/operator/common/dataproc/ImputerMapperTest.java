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

public class ImputerMapperTest {

    TableSchema modelSchema = new TableSchema(new String[]{"model_id", "model_info", "f_double", "f_long", "f_int"},
        new TypeInformation[]{Types.LONG, Types.STRING, Types.DOUBLE, Types.LONG, Types.INT});

    @Test
    public void testMean() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0L, "{\"selectedCols\":\"[\\\"f_double\\\",\\\"f_long\\\",\\\"f_int\\\"]\",\"strategy\":\"\\\"mean\\\"\"}", null, null, null),
            Row.of(1048576L, "[0.3333333333333333,1.0,1.0]", null, null, null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"f_string", "f_long", "f_int", "f_double", "f_boolean"},
            new TypeInformation<?>[]{Types.STRING, Types.LONG, Types.INT, Types.DOUBLE, Types.BOOLEAN}
        );
        Params params = new Params();

        ImputerModelMapper mapper = new ImputerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a", null, null, null, true)).getField(1), 1L);
        assertEquals(mapper.map(Row.of("a", null, null, null, true)).getField(2), 1);
        assertEquals((double) mapper.map(Row.of("a", null, null, null, true)).getField(3), 0.333333333, 10e-4);
    }

    @Test
    public void testMin() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0L, "{\"selectedCols\":\"[\\\"f_double\\\",\\\"f_long\\\",\\\"f_int\\\"]\",\"strategy\":\"\\\"min\\\"\"}", null, null, null),
            Row.of(1048576L, "[-3.0,0.0,0.0]", null, null, null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"f_string", "f_long", "f_int", "f_double", "f_boolean"},
            new TypeInformation<?>[]{Types.STRING, Types.LONG, Types.INT, Types.DOUBLE, Types.BOOLEAN}
        );
        Params params = new Params();

        ImputerModelMapper mapper = new ImputerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a", null, null, null, true)).getField(1), 0L);
        assertEquals(mapper.map(Row.of("a", null, null, null, true)).getField(2), 0);
        assertEquals((double) mapper.map(Row.of("a", null, null, null, true)).getField(3), -3.0, 10e-4);
    }

    @Test
    public void testMax() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0L, "{\"selectedCols\":\"[\\\"f_double\\\",\\\"f_long\\\",\\\"f_int\\\"]\",\"strategy\":\"\\\"min\\\"\"}", null, null, null),
            Row.of(1048576L, "[2.0, 2.0, 2.0]", null, null, null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"f_string", "f_long", "f_int", "f_double", "f_boolean"},
            new TypeInformation<?>[]{Types.STRING, Types.LONG, Types.INT, Types.DOUBLE, Types.BOOLEAN}
        );
        Params params = new Params();

        ImputerModelMapper mapper = new ImputerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a", null, null, null, true)).getField(1), 2L);
        assertEquals(mapper.map(Row.of("a", null, null, null, true)).getField(2), 2);
        assertEquals((double) mapper.map(Row.of("a", null, null, null, true)).getField(3), 2.0, 10e-4);
    }

    @Test
    public void testValue() throws Exception {
        TableSchema modelSchema = new TableSchema(new String[]{"model_id", "model_info", "f_double", "f_long", "f_int", "f_boolean"},
                new TypeInformation[]{Types.LONG, Types.STRING, Types.DOUBLE, Types.LONG, Types.INT, Types.BOOLEAN});
        Row[] rows = new Row[]{
                Row.of(0L, "{\"selectedCols\":\"[\\\"f_double\\\",\\\"f_long\\\",\\\"f_int\\\",\\\"f_boolean\\\"]\",\"strategy\":\"\\\"0\\\"\"}", null, null, null),
        };
        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"f_string", "f_long", "f_int", "f_double", "f_boolean"},
            new TypeInformation<?>[]{Types.STRING, Types.LONG, Types.INT, Types.DOUBLE, Types.BOOLEAN}
        );
        Params params = new Params();

        ImputerModelMapper mapper = new ImputerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of("a", null, null, null, null)).getField(1), 0L);
        assertEquals(mapper.map(Row.of("a", null, null, null, null)).getField(2), 0);
        assertEquals((double) mapper.map(Row.of("a", null, null, null, null)).getField(3), 0.0, 10e-4);
        assertEquals(mapper.map(Row.of("a", null, null, null, null)).getField(4), false);
    }
}