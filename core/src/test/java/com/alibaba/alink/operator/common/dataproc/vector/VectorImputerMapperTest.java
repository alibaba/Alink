package com.alibaba.alink.operator.common.dataproc.vector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class VectorImputerMapperTest {

    TableSchema modelSchema = new TableSchema(new String[]{"model_id", "model_info", "vec"},
        new TypeInformation[]{Types.LONG, Types.STRING, Types.STRING});

    @Test
    public void testMean() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0L, "{\"selectedCol\":\"\\\"vec\\\"\",\"strategy\":\"\\\"mean\\\"\"}", null),
            Row.of(1048576L, "[1.3333333333333333,-0.3333333333333333]", null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"vec"},
            new TypeInformation<?>[]{Types.STRING}
        );
        Params params = new Params();

        VectorImputerModelMapper mapper = new VectorImputerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of(new DenseVector(new double[]{1.0, Double.NaN}))).getField(0),
                new DenseVector(new double[]{1.0, -0.3333333333333333}));
    }

    @Test
    public void testMin() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0L, "{\"selectedCol\":\"\\\"vec\\\"\",\"strategy\":\"\\\"min\\\"\"}", null),
            Row.of(1048576L, "[1.3333333333333333,-3.0]", null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"vec"},
            new TypeInformation<?>[]{Types.STRING}
        );
        Params params = new Params();

        VectorImputerModelMapper mapper = new VectorImputerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of(new DenseVector(new double[]{1.0, Double.NaN}))).getField(0),
                new DenseVector(new double[]{1.0, -3.0}));
    }

    @Test
    public void testMax() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0L, "{\"selectedCol\":\"\\\"vec\\\"\",\"strategy\":\"\\\"max\\\"\"}", null),
            Row.of(1048576L, "[1.3333333333333333,2.0]", null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"vec"},
            new TypeInformation<?>[]{Types.STRING}
        );
        Params params = new Params();

        VectorImputerModelMapper mapper = new VectorImputerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of(new DenseVector(new double[]{1.0, Double.NaN}))).getField(0),
                new DenseVector(new double[]{1.0, 2.0}));
    }

    @Test
    public void testSparseMax() throws Exception {
        Row[] rows = new Row[]{
                Row.of(0L, "{\"selectedCol\":\"\\\"vec\\\"\",\"strategy\":\"\\\"max\\\"\"}", null),
                Row.of(1048576L, "[1.3333333333333333,0.0,2.0]", null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
                new String[]{"vec"},
                new TypeInformation<?>[]{Types.STRING}
        );
        Params params = new Params();

        VectorImputerModelMapper mapper = new VectorImputerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of(new SparseVector(3, new int[]{0,2}, new double[]{1.0, Double.NaN}))).getField(0),
                new SparseVector(3, new int[]{0,2}, new double[]{1.0, 2.0}));
    }

    @Test
    public void testValue() throws Exception {
        Row[] rows = new Row[]{
                Row.of(0L, "{\"selectedCol\":\"\\\"vec\\\"\",\"strategy\":\"\\\"-7.0\\\"\"}", null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"vec"},
            new TypeInformation<?>[]{Types.STRING}
        );
        Params params = new Params();

        VectorImputerModelMapper mapper = new VectorImputerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of(new SparseVector(3, new int[]{0,2}, new double[]{1.0, Double.NaN}))).getField(0),
                new SparseVector(3, new int[]{0,2}, new double[]{1.0, -7.0}));
    }

}