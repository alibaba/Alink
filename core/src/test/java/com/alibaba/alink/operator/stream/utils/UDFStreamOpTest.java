package com.alibaba.alink.operator.stream.utils;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.Assert;
import org.junit.Test;

public class UDFStreamOpTest {

    public static class LengthPlusValue extends ScalarFunction {
        public long eval(String s, long v) {
            return s.length() + v;
        }

        @Override
        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return Types.LONG;
        }
    }

    @Test
    public void testDefaultReservedCols() throws Exception {
        MemSourceStreamOp src = new MemSourceStreamOp(new Object[][]{
            new Object[]{"1", "a", 1L},
            new Object[]{"2", "b33", 2L}
        }, new String[]{"c0", "c1", "c2"});

        UDFStreamOp udfOp = new UDFStreamOp()
            .setFunc(new UDFStreamOpTest.LengthPlusValue())
            .setSelectedCols("c1", "c2")
            .setOutputCol("c2");

        udfOp.linkFrom(src);

        Assert.assertArrayEquals(new String[]{"c0", "c1", "c2"}, udfOp.getColNames());
        udfOp.print();
        StreamOperator.execute();
    }

    @Test
    public void testEmptyReservedCols() throws Exception {
        MemSourceStreamOp src = new MemSourceStreamOp(new Object[][]{
            new Object[]{"1", "a", 1L},
            new Object[]{"2", "b33", 2L}
        }, new String[]{"c0", "c1", "c2"});

        UDFStreamOp udfOp = new UDFStreamOp()
            .setFunc(new UDFStreamOpTest.LengthPlusValue())
            .setSelectedCols("c1", "c2")
            .setReservedCols(new String[]{})
            .setOutputCol("c2");

        udfOp.linkFrom(src);

        Assert.assertArrayEquals(new String[]{"c2"}, udfOp.getColNames());
        udfOp.print();
        StreamOperator.execute();
    }

}