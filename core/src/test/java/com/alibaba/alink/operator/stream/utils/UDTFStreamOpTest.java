package com.alibaba.alink.operator.stream.utils;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;
import org.junit.Assert;
import org.junit.Test;

public class UDTFStreamOpTest {

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
        MemSourceStreamOp src = new MemSourceStreamOp(new Object[][]{
            new Object[]{"1", "a b", 1L},
            new Object[]{"2", "b33 bb44", 2L},
            new Object[]{"3", "", 3L}
        }, new String[]{"c0", "c1", "c2"});

        UDTFStreamOp udtfOp = new UDTFStreamOp()
            .setFunc(new Split(" "))
            .setSelectedCols("c1", "c2")
            .setReservedCols(new String[]{"c1", "c2"})
            .setOutputCols("c1", "length");
        udtfOp.linkFrom(src);

        Assert.assertArrayEquals(new String[]{"c2", "c1", "length"}, udtfOp.getColNames());
        udtfOp.print();

        StreamOperator.execute();
    }
}
