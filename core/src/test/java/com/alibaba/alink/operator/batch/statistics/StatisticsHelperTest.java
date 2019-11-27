package com.alibaba.alink.operator.batch.statistics;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.TableSourceBatchOp;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.statistics.StatisticsHelper;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class StatisticsHelperTest {

    @Test
    public void summaryHelperTable() throws Exception {
        BatchOperator data = getBatchTable();
        String[] selectedColNames = new String[]{"f_long", "f_int", "f_double"};
        Tuple2<DataSet<Vector>, DataSet<BaseVectorSummary>> dataSet =
            StatisticsHelper.summaryHelper(data, selectedColNames, null);

        BaseVectorSummary summary = dataSet.f1.collect().get(0);

        assertEquals(summary.vectorSize(), 3);
        assertEquals(summary.count(), 4);
        assertEquals(summary.max(2), 4.0, 10e-4);
        assertEquals(summary.min(1), 0.0, 10e-4);
        assertEquals(summary.mean(2), 1.25, 10e-4);
        assertEquals(summary.variance(2), 8.9167, 10e-4);
        assertEquals(summary.standardDeviation(2), 2.9861, 10e-4);
        assertEquals(summary.normL1(2), 11.0, 10e-4);
        assertEquals(summary.normL2(2), 5.7446, 10e-4);

        List<Vector> vectors = dataSet.f0.collect();

        assertEquals(vectors.size(), 4);

        assertArrayEquals(((DenseVector) vectors.get(0)).getData(), new double[]{1, 1, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(1)).getData(), new double[]{2, 2, -3.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(2)).getData(), new double[]{1, 3, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(3)).getData(), new double[]{0, 0, 4.0}, 10e-4);
    }

    @Test
    public void summaryHelperVector() throws Exception {
        BatchOperator data = getDenseBatch();
        String vectorColName = "vec";

        Tuple2<DataSet<Vector>, DataSet<BaseVectorSummary>> dataSet =
            StatisticsHelper.summaryHelper(data, null, vectorColName);

        BaseVectorSummary summary = dataSet.f1.collect().get(0);

        assertEquals(summary.vectorSize(), 3);
        assertEquals(summary.count(), 4);
        assertEquals(summary.max(2), 4.0, 10e-4);
        assertEquals(summary.min(1), 0.0, 10e-4);
        assertEquals(summary.mean(2), 1.25, 10e-4);
        assertEquals(summary.variance(2), 8.9167, 10e-4);
        assertEquals(summary.standardDeviation(2), 2.9861, 10e-4);
        assertEquals(summary.normL1(2), 11.0, 10e-4);
        assertEquals(summary.normL2(2), 5.7446, 10e-4);

        List<Vector> vectors = dataSet.f0.collect();

        assertEquals(vectors.size(), 4);

        assertArrayEquals(((DenseVector) vectors.get(0)).getData(), new double[]{1, 1, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(1)).getData(), new double[]{2, 2, -3.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(2)).getData(), new double[]{1, 3, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(3)).getData(), new double[]{0, 0, 4.0}, 10e-4);
    }

    @Test
    public void summaryHelperTableWithReservedCols() throws Exception {
        BatchOperator data = getBatchTable();
        String[] selectedColNames = new String[]{"f_long", "f_int", "f_double"};
        String[] reservedColNames = new String[]{"id"};

        Tuple2<DataSet<Tuple2<Vector, Row>>, DataSet<BaseVectorSummary>> dataSet =
            StatisticsHelper.summaryHelper(data, selectedColNames, null, reservedColNames);

        BaseVectorSummary summary = dataSet.f1.collect().get(0);

        assertEquals(summary.vectorSize(), 3);
        assertEquals(summary.count(), 4);
        assertEquals(summary.max(2), 4.0, 10e-4);
        assertEquals(summary.min(1), 0.0, 10e-4);
        assertEquals(summary.mean(2), 1.25, 10e-4);
        assertEquals(summary.variance(2), 8.9167, 10e-4);
        assertEquals(summary.standardDeviation(2), 2.9861, 10e-4);
        assertEquals(summary.normL1(2), 11.0, 10e-4);
        assertEquals(summary.normL2(2), 5.7446, 10e-4);

        List<Tuple2<Vector, Row>> tuple2s = dataSet.f0.collect();

        assertEquals(tuple2s.size(), 4);

        assertArrayEquals(((DenseVector) tuple2s.get(0).f0).getData(), new double[]{1, 1, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(1).f0).getData(), new double[]{2, 2, -3.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(2).f0).getData(), new double[]{1, 3, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(3).f0).getData(), new double[]{0, 0, 4.0}, 10e-4);

        assertEquals(tuple2s.get(0).f1.getField(0), 1);
        assertEquals(tuple2s.get(1).f1.getField(0), 2);
        assertEquals(tuple2s.get(2).f1.getField(0), 3);
        assertEquals(tuple2s.get(3).f1.getField(0), 4);
    }

    @Test
    public void summaryHelperVectorWithReservedCols() throws Exception {
        BatchOperator data = getDenseBatch();
        String vectorColName = "vec";
        String[] reservedColNames = new String[]{"id"};

        Tuple2<DataSet<Tuple2<Vector, Row>>, DataSet<BaseVectorSummary>> dataSet =
            StatisticsHelper.summaryHelper(data, null, vectorColName, reservedColNames);

        BaseVectorSummary summary = dataSet.f1.collect().get(0);

        assertEquals(summary.vectorSize(), 3);
        assertEquals(summary.count(), 4);
        assertEquals(summary.max(2), 4.0, 10e-4);
        assertEquals(summary.min(1), 0.0, 10e-4);
        assertEquals(summary.mean(2), 1.25, 10e-4);
        assertEquals(summary.variance(2), 8.9167, 10e-4);
        assertEquals(summary.standardDeviation(2), 2.9861, 10e-4);
        assertEquals(summary.normL1(2), 11.0, 10e-4);
        assertEquals(summary.normL2(2), 5.7446, 10e-4);

        List<Tuple2<Vector, Row>> tuple2s = dataSet.f0.collect();

        assertEquals(tuple2s.size(), 4);

        assertArrayEquals(((DenseVector) tuple2s.get(0).f0).getData(), new double[]{1, 1, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(1).f0).getData(), new double[]{2, 2, -3.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(2).f0).getData(), new double[]{1, 3, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(3).f0).getData(), new double[]{0, 0, 4.0}, 10e-4);

        assertEquals(tuple2s.get(0).f1.getField(0), 1);
        assertEquals(tuple2s.get(1).f1.getField(0), 2);
        assertEquals(tuple2s.get(2).f1.getField(0), 3);
        assertEquals(tuple2s.get(3).f1.getField(0), 4);
    }

    @Test
    public void summary() throws Exception {
        BatchOperator data = getBatchTable();
        String[] selectedColNames = new String[]{"f_long", "f_int", "f_double"};
        DataSet<TableSummary> dataSet =
            StatisticsHelper.summary(data, selectedColNames);

        TableSummary summary = dataSet.collect().get(0);

        assertArrayEquals(summary.getColNames(), selectedColNames);
        assertEquals(summary.count(), 4);
        assertEquals(summary.max("f_double"), 4.0, 10e-4);
        assertEquals(summary.min("f_int"), 0.0, 10e-4);
        assertEquals(summary.mean("f_double"), 1.25, 10e-4);
        assertEquals(summary.variance("f_double"), 8.9167, 10e-4);
        assertEquals(summary.standardDeviation("f_double"), 2.9861, 10e-4);
        assertEquals(summary.normL1("f_double"), 11.0, 10e-4);
        assertEquals(summary.normL2("f_double"), 5.7446, 10e-4);
    }

    @Test
    public void vectorSummary() throws Exception {
        BatchOperator data = getDenseBatch();
        String vectorColName = "vec";

        DataSet<BaseVectorSummary> dataSet =
            StatisticsHelper.vectorSummary(data, vectorColName);

        BaseVectorSummary summary = dataSet.collect().get(0);

        assertEquals(summary.vectorSize(), 3);
        assertEquals(summary.count(), 4);
        assertEquals(summary.max(2), 4.0, 10e-4);
        assertEquals(summary.min(1), 0.0, 10e-4);
        assertEquals(summary.mean(2), 1.25, 10e-4);
        assertEquals(summary.variance(2), 8.9167, 10e-4);
        assertEquals(summary.standardDeviation(2), 2.9861, 10e-4);
        assertEquals(summary.normL1(2), 11.0, 10e-4);
        assertEquals(summary.normL2(2), 5.7446, 10e-4);
    }

    @Test
    public void dataSetSummary() throws Exception {
        BatchOperator data = getDenseBatch();

        DataSet<BaseVectorSummary> dataSet =
            StatisticsHelper.summary(data.getDataSet()
                .map((MapFunction<Row, Vector>) in -> VectorUtil.getVector(in.getField(1))));

        BaseVectorSummary summary = dataSet.collect().get(0);

        assertEquals(summary.vectorSize(), 3);
        assertEquals(summary.count(), 4);
        assertEquals(summary.max(2), 4.0, 10e-4);
        assertEquals(summary.min(1), 0.0, 10e-4);
        assertEquals(summary.mean(2), 1.25, 10e-4);
        assertEquals(summary.variance(2), 8.9167, 10e-4);
        assertEquals(summary.standardDeviation(2), 2.9861, 10e-4);
        assertEquals(summary.normL1(2), 11.0, 10e-4);
        assertEquals(summary.normL2(2), 5.7446, 10e-4);
    }

    @Test
    public void pearsonCorrelation() throws Exception {
        BatchOperator data = getBatchTable();
        String[] selectedColNames = new String[]{"f_long", "f_int", "f_double"};

        DataSet<Tuple2<TableSummary, CorrelationResult>> dataSet =
            StatisticsHelper.pearsonCorrelation(data, selectedColNames);

        Tuple2<TableSummary, CorrelationResult> tuple2 = dataSet.collect().get(0);
        TableSummary summary = tuple2.f0;
        CorrelationResult corr = tuple2.f1;

        assertArrayEquals(summary.getColNames(), selectedColNames);
        assertEquals(summary.count(), 4);
        assertEquals(summary.max("f_double"), 4.0, 10e-4);
        assertEquals(summary.min("f_int"), 0.0, 10e-4);
        assertEquals(summary.mean("f_double"), 1.25, 10e-4);
        assertEquals(summary.variance("f_double"), 8.9167, 10e-4);
        assertEquals(summary.standardDeviation("f_double"), 2.9861, 10e-4);
        assertEquals(summary.normL1("f_double"), 11.0, 10e-4);
        assertEquals(summary.normL2("f_double"), 5.7446, 10e-4);

        assertArrayEquals(corr.getCorrelationMatrix().getArrayCopy1D(true),
            new double[]{1.0, 0.6325, -0.9570, 0.6325, 1.0, -0.4756, -0.9570, -0.4756, 1.0}, 10e-4);
    }

    @Test
    public void vectorPearsonCorrelation() throws Exception {
        BatchOperator data = getDenseBatch();
        String vectorColName = "vec";

        DataSet<Tuple2<BaseVectorSummary, CorrelationResult>> dataSet =
            StatisticsHelper.vectorPearsonCorrelation(data, vectorColName);

        Tuple2<BaseVectorSummary, CorrelationResult> tuple2 = dataSet.collect().get(0);
        BaseVectorSummary summary = tuple2.f0;
        CorrelationResult corr = tuple2.f1;

        assertEquals(summary.vectorSize(), 3);
        assertEquals(summary.count(), 4);
        assertEquals(summary.max(2), 4.0, 10e-4);
        assertEquals(summary.min(1), 0.0, 10e-4);
        assertEquals(summary.mean(2), 1.25, 10e-4);
        assertEquals(summary.variance(2), 8.9167, 10e-4);
        assertEquals(summary.standardDeviation(2), 2.9861, 10e-4);
        assertEquals(summary.normL1(2), 11.0, 10e-4);
        assertEquals(summary.normL2(2), 5.7446, 10e-4);


        assertArrayEquals(corr.getCorrelationMatrix().getArrayCopy1D(true),
            new double[]{1.0, 0.6325, -0.9570, 0.6325, 1.0, -0.4756, -0.9570, -0.4756, 1.0}, 10e-4);
    }

    @Test
    public void transformTableToVector() throws Exception {
        BatchOperator data = getBatchTable();
        String[] selectedColNames = new String[]{"f_long", "f_int", "f_double"};
        DataSet<Vector> dataSet =
            StatisticsHelper.transformToVector(data, selectedColNames, null);

        List<Vector> vectors = dataSet.collect();

        assertEquals(vectors.size(), 4);

        assertArrayEquals(((DenseVector) vectors.get(0)).getData(), new double[]{1, 1, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(1)).getData(), new double[]{2, 2, -3.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(2)).getData(), new double[]{1, 3, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(3)).getData(), new double[]{0, 0, 4.0}, 10e-4);
    }

    @Test
    public void transformVectorToVector() throws Exception {
        BatchOperator data = getDenseBatch();
        String vectorColName = "vec";
        DataSet<Vector> dataSet =
            StatisticsHelper.transformToVector(data, null, vectorColName);

        List<Vector> vectors = dataSet.collect();

        assertEquals(vectors.size(), 4);

        assertArrayEquals(((DenseVector) vectors.get(0)).getData(), new double[]{1, 1, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(1)).getData(), new double[]{2, 2, -3.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(2)).getData(), new double[]{1, 3, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) vectors.get(3)).getData(), new double[]{0, 0, 4.0}, 10e-4);
    }

    @Test
    public void transformTableToVectorWithReservedCols() throws Exception {
        BatchOperator data = getBatchTable();
        String[] selectedColNames = new String[]{"f_long", "f_int", "f_double"};
        DataSet<Tuple2<Vector, Row>> dataSet =
            StatisticsHelper.transformToVector(data, selectedColNames, null, new String[]{"id"});

        List<Tuple2<Vector, Row>> tuple2s = dataSet.collect();

        assertEquals(tuple2s.size(), 4);

        assertArrayEquals(((DenseVector) tuple2s.get(0).f0).getData(), new double[]{1, 1, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(1).f0).getData(), new double[]{2, 2, -3.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(2).f0).getData(), new double[]{1, 3, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(3).f0).getData(), new double[]{0, 0, 4.0}, 10e-4);

        assertEquals(tuple2s.get(0).f1.getField(0), 1);
        assertEquals(tuple2s.get(1).f1.getField(0), 2);
        assertEquals(tuple2s.get(2).f1.getField(0), 3);
        assertEquals(tuple2s.get(3).f1.getField(0), 4);
    }

    @Test
    public void transformVectorToVectorWithReservedCols() throws Exception {
        BatchOperator data = getDenseBatch();
        String vectorColName = "vec";
        DataSet<Tuple2<Vector, Row>> dataSet =
            StatisticsHelper.transformToVector(data, null, vectorColName, new String[]{"id"});

        List<Tuple2<Vector, Row>> tuple2s = dataSet.collect();

        assertEquals(tuple2s.size(), 4);

        assertArrayEquals(((DenseVector) tuple2s.get(0).f0).getData(), new double[]{1, 1, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(1).f0).getData(), new double[]{2, 2, -3.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(2).f0).getData(), new double[]{1, 3, 2.0}, 10e-4);
        assertArrayEquals(((DenseVector) tuple2s.get(3).f0).getData(), new double[]{0, 0, 4.0}, 10e-4);

        assertEquals(tuple2s.get(0).f1.getField(0), 1);
        assertEquals(tuple2s.get(1).f1.getField(0), 2);
        assertEquals(tuple2s.get(2).f1.getField(0), 3);
        assertEquals(tuple2s.get(3).f1.getField(0), 4);
    }

    @Test
    public void transformTableToColumns() throws Exception {
        BatchOperator data = getBatchTable();
        String[] selectedColNames = new String[]{"f_long", "f_int", "f_double"};
        DataSet<Row> dataSet =
            StatisticsHelper.transformToColumns(data, selectedColNames, null, new String[]{"id"});

        List<Row> rows = dataSet.collect();

        assertEquals(rows.size(), 4);

        assertEquals(rows.get(0).getField(3), 1);
        assertEquals(rows.get(1).getField(3), 2);
        assertEquals(rows.get(2).getField(3), 3);
        assertEquals(rows.get(3).getField(3), 4);

        assertEquals((double) rows.get(0).getField(1), 1.0, 10e-4);
        assertEquals((double) rows.get(1).getField(1), 2.0, 10e-4);
        assertEquals((double) rows.get(2).getField(1), 3.0, 10e-4);
        assertEquals((double) rows.get(3).getField(1), 0.0, 10e-4);
    }

    @Test
    public void transformVectorToColumns() throws Exception {
        BatchOperator data = getDenseBatch();
        String vectorColName = "vec";
        DataSet<Row> dataSet =
            StatisticsHelper.transformToColumns(data, null, vectorColName, new String[]{"id"});

        List<Row> rows = dataSet.collect();

        assertEquals(rows.size(), 4);

        assertEquals(rows.get(0).getField(3), 1);
        assertEquals(rows.get(1).getField(3), 2);
        assertEquals(rows.get(2).getField(3), 3);
        assertEquals(rows.get(3).getField(3), 4);

        assertEquals((double) rows.get(0).getField(1), 1.0, 10e-4);
        assertEquals((double) rows.get(1).getField(1), 2.0, 10e-4);
        assertEquals((double) rows.get(2).getField(1), 3.0, 10e-4);
        assertEquals((double) rows.get(3).getField(1), 0.0, 10e-4);
    }


    private static BatchOperator getBatchTable() {
        Row[] testArray =
            new Row[]{
                Row.of(1, 1L, 1, 2.0),
                Row.of(2, 2L, 2, -3.0),
                Row.of(3, 1L, 3, 2.0),
                Row.of(4, 0L, 0, 4.0),
            };

        String[] colNames = new String[]{"id", "f_long", "f_int", "f_double"};

        return new TableSourceBatchOp(MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames));
    }

    private static BatchOperator getDenseBatch() {
        Row[] testArray =
            new Row[]{
                Row.of(1, "1.0 1.0 2.0"),
                Row.of(2, "2.0 2.0 -3.0"),
                Row.of(3, "1.0 3.0 2.0"),
                Row.of(4, "0.0 0.0 4.0"),
            };

        String selectedColName = "vec";
        String[] colNames = new String[]{"id", selectedColName};

        return new TableSourceBatchOp(MLEnvironmentFactory.getDefault().createBatchTable(Arrays.asList(testArray), colNames));
    }
}