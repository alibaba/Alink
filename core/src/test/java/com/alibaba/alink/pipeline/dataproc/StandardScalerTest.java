package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.AlgoOperator;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import org.apache.flink.api.common.typeinfo.Types;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StandardScalerTest {

    public static AlgoOperator getMultiTypeData(boolean isBatch) {
        Row[] testArray =
            new Row[]{
                Row.of(new Object[]{"0", "a", 1L, 1, 0.2, true}),
                Row.of(new Object[]{"1", null, 2L, 2, null, true}),
                Row.of(new Object[]{"2", "c", null, null, null, false}),
                Row.of(new Object[]{"3", "a", 0L, 0, null, null}),
            };

        String[] colNames = new String[]{"id", "f_string", "f_long", "f_int", "f_double", "f_boolean"};
        TypeInformation[] colTypes = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG, Types.INT, Types.DOUBLE,
            Types.BOOLEAN};
        TableSchema schema = new TableSchema(
            colNames,
            colTypes
        );

        if (isBatch) {
            return new MemSourceBatchOp(Arrays.asList(testArray), schema);
        } else {
            return new MemSourceStreamOp(Arrays.asList(testArray), schema);
        }
    }

    @Test
    public void test() throws Exception {
        BatchOperator batchData = (BatchOperator) getMultiTypeData(true);
        StreamOperator streamData = (StreamOperator) getMultiTypeData(false);
        String[] selectedColNames = new String[]{"f_long", "f_int", "f_double"};

        StandardScaler scaler = new StandardScaler()
            .setSelectedCols(selectedColNames)
            .setWithMean(true)
            .setWithStd(true);

        StandardScalerModel model = scaler.fit(batchData);
        BatchOperator res = model.transform(batchData);

        List rows = res.getDataSet().collect();
        HashMap<String, Tuple3<Long, Integer, Double>> map = new HashMap<String, Tuple3<Long, Integer, Double>>();
        map.put((String) ((Row) rows.get(0)).getField(0), Tuple3.of(
            (((Row) rows.get(0)).getField(2)) == null? null: ((Double) ((Row) rows.get(0)).getField(2)).longValue(),
            (((Row) rows.get(0)).getField(3)) == null? null: ((Double) ((Row) rows.get(0)).getField(3)).intValue(),
            (Double) ((Row) rows.get(0)).getField(4)));
        map.put((String) ((Row) rows.get(1)).getField(0), Tuple3.of(
            (((Row) rows.get(1)).getField(2)) == null? null: ((Double) ((Row) rows.get(1)).getField(2)).longValue(),
            (((Row) rows.get(1)).getField(3)) == null? null: ((Double) ((Row) rows.get(1)).getField(3)).intValue(),
            (Double) ((Row) rows.get(1)).getField(4)));
        map.put((String) ((Row) rows.get(2)).getField(0), Tuple3.of(
            (((Row) rows.get(2)).getField(2)) == null? null: ((Double) ((Row) rows.get(2)).getField(2)).longValue(),
            (((Row) rows.get(2)).getField(3)) == null? null: ((Double) ((Row) rows.get(2)).getField(3)).intValue(),
            (Double) ((Row) rows.get(2)).getField(4)));
        map.put((String) ((Row) rows.get(3)).getField(0), Tuple3.of(
            (((Row) rows.get(3)).getField(2)) == null? null: ((Double) ((Row) rows.get(3)).getField(2)).longValue(),
            (((Row) rows.get(3)).getField(3)) == null? null: ((Double) ((Row) rows.get(3)).getField(3)).intValue(),
            (Double) ((Row) rows.get(3)).getField(4)));
        assertEquals(map.get("0"), new Tuple3<>(0L, 0, 0.0));
        assertEquals(map.get("1"), new Tuple3<>(1L, 1, null));
        assertEquals(map.get("2"), new Tuple3<>(null, null, null));
        assertEquals(map.get("3"), new Tuple3<>(-1L, -1, null));

        model.transform(streamData).print();
        StreamOperator.execute();
    }
}