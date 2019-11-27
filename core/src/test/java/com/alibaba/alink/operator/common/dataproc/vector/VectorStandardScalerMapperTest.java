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

public class VectorStandardScalerMapperTest {
    private TableSchema modelSchema = new TableSchema(new String[]{"model_id", "model_info", "vec"},
        new TypeInformation[]{Types.LONG, Types.STRING, Types.STRING});

    @Test
    public void testSparse() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0L, "{\"withMean\":\"true\",\"selectedCol\":\"\\\"vec\\\"\",\"withStd\":\"true\"}", null),
            Row.of(1048576L, "[1.3333333333333333,1.0,0.3333333333333333]", null),
            Row.of(2097152L, "[2.5166114784235836,1.0,2.886751345948129]", null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"vec"},
            new TypeInformation<?>[]{Types.STRING}
        );
        Params params = new Params();

        VectorStandardScalerModelMapper mapper = new VectorStandardScalerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of(new SparseVector(3, new int[]{0, 2}, new double[]{1.0, 2.0}))).getField(0),
                new DenseVector(new double[]{-0.13245323570650433, -1.0, 0.5773502691896257}));
    }


    @Test
    public void testDense() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0L, "{\"withMean\":\"true\",\"selectedCol\":\"\\\"vec\\\"\",\"withStd\":\"true\"}", null),
            Row.of(1048576L, "[1.3333333333333333,0.3333333333333333]", null),
            Row.of(2097152L, "[2.5166114784235836,2.886751345948129]", null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"vec"},
            new TypeInformation<?>[]{Types.STRING}
        );
        Params params = new Params();

        VectorStandardScalerModelMapper mapper = new VectorStandardScalerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals(mapper.map(Row.of(new DenseVector(new double[]{1.0, 2.0}))).getField(0),
                new DenseVector(new double[]{-0.13245323570650433, 0.5773502691896257}));
    }
}