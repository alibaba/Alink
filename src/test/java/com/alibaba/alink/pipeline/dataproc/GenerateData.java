package com.alibaba.alink.pipeline.dataproc;

import com.alibaba.alink.common.MLEnvironmentFactory;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * GenerateData class for test.
 */
public class GenerateData {

    public static Table getBatchTable() {
        Row[] testArray =
            new Row[]{
                Row.of(1.0, 2.0),
                Row.of(-1.0, -3.0),
                Row.of(4.0, 2.0),
                Row.of(null, null),
            };

        String[] colNames = new String[]{"f0", "f1"};

        return MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);
    }

    public static Table getStreamTable() {
        Row[] testArray =
            new Row[]{
                Row.of(1.0, 2.0),
                Row.of(-1.0, -3.0),
                Row.of(4.0, 2.0),
                Row.of(null, null),
            };

        String[] colNames = new String[]{"f0", "f1"};

        return MLEnvironmentFactory.getDefault().createStreamTable(Arrays.asList(testArray), colNames);
    }

    public static Table getMultiTypeBatchTable() {
        Row[] testArray =
            new Row[]{
                Row.of("a", 1L, 1, 2.0, true),
                Row.of(null, 2L, 2, -3.0, true),
                Row.of("c", null, null, 2.0, false),
                Row.of("a", 0L, 0, null, null),
            };

        String[] colNames = new String[]{"f_string", "f_long", "f_int", "f_double", "f_boolean"};

        return MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);
    }

    public static Table getMultiTypeStreamTable() {
        Row[] testArray =
            new Row[]{
                Row.of("a", 1L, 1, 2.0, true),
                Row.of(null, 2L, 2, -3.0, true),
                Row.of("c", null, null, 2.0, false),
                Row.of("a", 0L, 0, null, null),
            };

        String[] colNames = new String[]{"f_string", "f_long", "f_int", "f_double", "f_boolean"};

        return MLEnvironmentFactory.getDefault().createStreamTable(Arrays.asList(testArray), colNames);
    }

    public static Table getSparseBatch() {
        Row[] testArray =
            new Row[]{
                Row.of("0:1.0  1:2.0"),
                Row.of("0:-1.0  1:-3.0"),
                Row.of("0:4.0  1:2.0"),
                Row.of("")
            };

        String selectedColName = "vec";
        String[] colNames = new String[]{selectedColName};

        return MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);
    }

    public static Table getSparseStream() {
        Row[] testArray =
            new Row[]{
                Row.of("0:1.0 1:2.0"),
                Row.of("0:-1.0 1:-3.0"),
                Row.of("0:4.0 1:2.0"),
                Row.of(""),
                Row.of(new Object[]{null})
            };

        String selectedColName = "vec";
        String[] colNames = new String[]{selectedColName};

        return MLEnvironmentFactory.getDefault().createStreamTable(Arrays.asList(testArray), colNames);
    }

    public static Table getDenseBatch() {
        Row[] testArray =
            new Row[]{
                Row.of("1.0 2.0"),
                Row.of("-1.0 -3.0"),
                Row.of("4.0 2.0"),
            };

        String selectedColName = "vec";
        String[] colNames = new String[]{selectedColName};

        return MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);
    }

    public static Table getDenseStream() {
        Row[] testArray =
            new Row[]{
                Row.of("1.0 2.0"),
                Row.of("-1.0 -3.0"),
                Row.of("4.0 2.0"),
                Row.of(""),
                Row.of(new Object[]{null})
            };

        String selectedColName = "vec";
        String[] colNames = new String[]{selectedColName};

        return MLEnvironmentFactory.getDefault().createStreamTable(Arrays.asList(testArray), colNames);
    }

    public static Table getDenseBatchWithStreamLabel() {
        Row[] testArray =
            new Row[]{
                Row.of("1.0 2.0 4.0", "a"),
                Row.of("-1.0 -3.0 4.0", "a"),
                Row.of("4.0 2.0 3.0", "b"),
                Row.of("3.4 5.1 5.0", "b")
            };

        String[] colNames = new String[]{"vec", "label"};

        return MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);
    }

    public static Table getDenseBatchWithDoubleLabel() {

        Row[] testArray = new Row[]{
            Row.of(7, "0.0  0.0  18.0  1.0", 1.0),
            Row.of(8, "0.0  1.0  12.0  0.0", 0.0),
            Row.of(9, "1.0  0.0  15.0  0.1", 0.0),
        };

        String[] colNames = new String[]{"id", "features", "clicked"};

        return MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);
    }

    public static Table genTableWithStringLabel() {
        Row[] testArray =
            new Row[]{
                Row.of("a", 1, 1.1, 1.2),
                Row.of("b", -2, 0.9, 1.0),
                Row.of("c", 100, -0.01, 1.0),
                Row.of("d", -99, 100.9, 0.1),
                Row.of("a", 1, 1.1, 1.2),
                Row.of("b", -2, 0.9, 1.0),
                Row.of("c", 100, -0.01, 0.2),
                Row.of("d", -99, 100.9, 0.3)
            };

        String[] colNames = new String[]{"col1", "col2", "col3", "col4"};

        return MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames);
    }

}
