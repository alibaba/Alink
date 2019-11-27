package com.alibaba.alink.operator.common.statistics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CrosstabTest {

    @Test
    public void convert() {

        Map<Tuple2<String, String>, Long> maps = new HashMap<>();
        maps.put(Tuple2.of("f0", "f1"), 2L);
        maps.put(Tuple2.of("f0", "f2"), 3L);
        maps.put(Tuple2.of("f1", "f2"), 4L);
        maps.put(Tuple2.of("f2", "f3"), 5L);

        Crosstab crossTable = Crosstab.convert(maps);

        List<String> rowTags = crossTable.rowTags;
        List<String> colTags = crossTable.colTags;

        assertEquals(3, rowTags.size());
        assertTrue(rowTags.contains("f0"));
        assertTrue(rowTags.contains("f1"));
        assertTrue(rowTags.contains("f2"));
        assertEquals(3.0, crossTable.data[rowTags.indexOf("f0")][colTags.indexOf("f2")], 10e-4);
    }

    @Test
    public void merge() {
        Map<Tuple2<String, String>, Long> maps = new HashMap<>();
        maps.put(Tuple2.of("f0", "f1"), 2L);
        maps.put(Tuple2.of("f0", "f2"), 3L);
        maps.put(Tuple2.of("f1", "f2"), 4L);
        maps.put(Tuple2.of("f2", "f3"), 5L);

        Crosstab crossTable = Crosstab.convert(maps);

        Crosstab crossTableCom = Crosstab.merge(crossTable, crossTable);

        List<String> rowTags = crossTable.rowTags;
        List<String> colTags = crossTable.colTags;

        assertEquals(6.0, crossTableCom.data[rowTags.indexOf("f0")][colTags.indexOf("f2")], 10e-4);
    }
}