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

public class MaxAbsScalerMapperTest {

    TableSchema modelSchema = new TableSchema(new String[]{"model_id", "model_info", "f0", "f1"},
        new TypeInformation[]{Types.LONG, Types.STRING, Types.DOUBLE, Types.DOUBLE});

    @Test
    public void testMaxAbsScaler() throws Exception {
        Row[] rows = new Row[]{
            Row.of(0L, "{\"selectedCols\":\"[\\\"f0\\\",\\\"f1\\\"]\"}", null, null),
            Row.of(1048576L, "[4.0,3.0]", null, null)
        };

        List<Row> model = Arrays.asList(rows);

        TableSchema dataSchema = new TableSchema(
            new String[]{"f0", "f1"},
            new TypeInformation<?>[]{Types.DOUBLE, Types.DOUBLE}
        );
        Params params = new Params();

        MaxAbsScalerModelMapper mapper = new MaxAbsScalerModelMapper(modelSchema, dataSchema, params);
        mapper.loadModel(model);

        assertEquals((double) mapper.map(Row.of(1.0, 2.0)).getField(0), 0.25, 10e-4);
        assertEquals((double) mapper.map(Row.of(1.0, 2.0)).getField(1), 0.6666666666666666, 10e-4);
    }

}