package com.alibaba.alink.operator.batch.source;

import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.List;

public class HiveSourceBatchOpTest {

    @Test
    public void test() throws Exception {
        HiveSourceBatchOp source = new HiveSourceBatchOp()
            .setHiveConfDir("/Users/fanhong/Downloads/apache-hive-2.0.1-bin/conf")
            .setHiveVersion("2.0.1")
            .setDbName("default")
            .setInputTableName("iris");
        List<Row> rows = source.collect();
        System.out.println(rows);
    }
}
