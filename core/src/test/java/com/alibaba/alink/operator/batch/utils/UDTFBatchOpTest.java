package com.alibaba.alink.operator.batch.utils;

import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UDTFBatchOpTest {

    public static class Split extends TableFunction<Tuple2<String, Long>> {
        private String separator = " ";

        public Split(String separator) {
            this.separator = separator;
        }

        public void eval(String str, long v) {
            if (str.length() <= 0) {
                return;
            }
            for (String s : str.split(separator)) {
                // use collect(...) to emit a row
                collect(new Tuple2<String, Long>(s, v + s.length()));
            }
        }

        @Override
        public TypeInformation<Tuple2<String, Long>> getResultType() {
            return Types.TUPLE(Types.STRING, Types.LONG);
        }
    }

    @Test
    public void testUdtf() throws Exception {
        MemSourceBatchOp src = new MemSourceBatchOp(new Object[][]{
            new Object[]{"1", "a b", 1L},
            new Object[]{"2", "b33 bb44", 2L},
            new Object[]{"3", "", 3L}
        }, new String[]{"c0", "c1", "c2"});

        UDTFBatchOp udtfOp = new UDTFBatchOp()
            .setFunc(new Split(" "))
            .setSelectedCols("c1", "c2")
            .setReservedCols(new String[]{"c1", "c2"})
            .setOutputCols("c1", "length");
        udtfOp.linkFrom(src);

        // the column c1 in the input table is shadowed
        Assert.assertArrayEquals(udtfOp.getColNames(), new String[]{"c2", "c1", "length"});

        List<Row> expected = new ArrayList<>(Arrays.asList(
            Row.of(1L, "a", 2L),
            Row.of(1L, "b", 2L),
            Row.of(2L, "b33", 5L),
            Row.of(2L, "bb44", 6L)
        ));
        List<Row> result = udtfOp.collect();
        Assert.assertEquals(result, expected);
    }
}
