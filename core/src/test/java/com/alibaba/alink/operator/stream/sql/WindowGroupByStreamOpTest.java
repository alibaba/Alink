package com.alibaba.alink.operator.stream.sql;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.MemSourceStreamOp;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WindowGroupByStreamOpTest {
    @Test
    public void testWindowGroupby() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(Row.of("k1", 2.0));
        rows.add(Row.of("k1", 3.0));
        rows.add(Row.of("k2", 3.5));
        rows.add(Row.of("k2", 3.0));

        StreamOperator data = new MemSourceStreamOp(rows, new String[]{"f1", "f2"});

        StreamOperator wg = new WindowGroupByStreamOp()
            .setWindowLength(10)
            .setWindowType("TUMBLE")
            .setSelectClause("f1,sum(f2) as f2_sum")
            .setGroupByClause("f1");

        StreamOperator result = data.link(wg);
        System.out.println(result.getSchema());
        Assert.assertEquals(result.getColNames().length, 4);
    }

}