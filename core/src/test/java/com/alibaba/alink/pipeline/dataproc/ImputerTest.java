package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import com.alibaba.alink.pipeline.TestUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ImputerTest {

    AlgoOperator getData(boolean isBatch) {


        Row[] testArray =
            new Row[]{
                Row.of("0", "a", 1L, 1, 2.0, true),
                Row.of("1", null, 2L, 2, -3.0, true),
                Row.of("2", "c", null, null, 2.0, false),
                Row.of("3", "a", 0L, 0, null, null),
            };

        String[] colNames = new String[]{"id", "f_string", "f_long", "f_int", "f_double", "f_boolean"};
        TableSchema schema = new TableSchema(
            colNames,
            new TypeInformation<?>[] {Types.STRING, Types.STRING, Types.LONG, Types.INT, Types.DOUBLE, Types.BOOLEAN}
        );
        if (isBatch) {
            return new MemSourceBatchOp(Arrays.asList(testArray), schema);
        } else {
            return new MemSourceStreamOp(Arrays.asList(testArray), schema);
        }

    }

    @Test
    public void testPipelineMean() throws Exception {
        String[] selectedColNames = new String[]{"f_double", "f_long", "f_int"};

        testPipeline(selectedColNames);
    }

    public void testPipeline(String[] selectedColNames) throws Exception {
        BatchOperator batchData = (BatchOperator) getData(true);

        Imputer fillMissingValue = new Imputer()
            .setSelectedCols(selectedColNames)
            .setStrategy("value")
            .setFillValue("1");

        ImputerModel model = fillMissingValue.fit(batchData);
        BatchOperator res = model.transform(batchData);

        List rows = res.getDataSet().collect();
        HashMap<String, Tuple3<Long, Integer, Double>> map = new HashMap<String, Tuple3<Long, Integer, Double>>();
        map.put((String) ((Row) rows.get(0)).getField(0), Tuple3.of(
            (Long) ((Row) rows.get(0)).getField(2),
            (Integer) ((Row) rows.get(0)).getField(3),
            (Double) ((Row) rows.get(0)).getField(4)));
        map.put((String) ((Row) rows.get(1)).getField(0), Tuple3.of(
            (Long) ((Row) rows.get(1)).getField(2),
            (Integer) ((Row) rows.get(1)).getField(3),
            (Double) ((Row) rows.get(1)).getField(4)));
        map.put((String) ((Row) rows.get(2)).getField(0), Tuple3.of(
            (Long) ((Row) rows.get(2)).getField(2),
            (Integer) ((Row) rows.get(2)).getField(3),
            (Double) ((Row) rows.get(2)).getField(4)));
        map.put((String) ((Row) rows.get(3)).getField(0), Tuple3.of(
            (Long) ((Row) rows.get(3)).getField(2),
            (Integer) ((Row) rows.get(3)).getField(3),
            (Double) ((Row) rows.get(3)).getField(4)));
        assertEquals(map.get("0"), new Tuple3<>(1L, 1, 2.0));
        assertEquals(map.get("1"), new Tuple3<>(2L, 2, -3.0));
        assertEquals(map.get("2"), new Tuple3<>(1L, 1, 2.0));
        assertEquals(map.get("3"), new Tuple3<>(0L, 0, 1.0));


        StreamOperator streamData = (StreamOperator) getData(false);
        model.transform(streamData).print();
        StreamOperator.execute();

    }

}
