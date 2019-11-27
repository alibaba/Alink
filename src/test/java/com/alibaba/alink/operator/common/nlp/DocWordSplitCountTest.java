package com.alibaba.alink.operator.common.nlp;

import com.alibaba.alink.common.MLEnvironmentFactory;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Unit test for DocWordSplitCount.
 */
public class DocWordSplitCountTest {
    @Test
    public void test() throws Exception {
        BatchTableEnvironment environment = MLEnvironmentFactory.getDefault().getBatchTableEnvironment();
        DataSet<Row> input = MLEnvironmentFactory.getDefault().getExecutionEnvironment().fromElements(Row.of("a b c d a b c"));
        Table table = environment.fromDataSet(input);
        environment.registerFunction("DocWordSplitCount", new DocWordSplitCount(" "));
        environment.registerTable("myTable", table);
        List<Row> list = environment.toDataSet(
            environment.sqlQuery("SELECT w, cnt FROM myTable, LATERAL TABLE(DocWordSplitCount(f0)) as T(w, cnt)"),
            Row.class).collect();
        Assert.assertArrayEquals(list.toArray(),
            new Row[] {Row.of("a", 2L), Row.of("b", 2L), Row.of("c", 2L), Row.of("d", 1L)});
    }

}