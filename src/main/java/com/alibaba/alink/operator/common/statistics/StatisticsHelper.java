package com.alibaba.alink.operator.common.statistics;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.alibaba.alink.common.linalg.Vector;
import com.alibaba.alink.common.linalg.VectorUtil;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.BaseVectorSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.CorrelationResult;
import com.alibaba.alink.operator.common.statistics.basicstatistic.DenseVectorSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummarizer;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.operator.common.statistics.basicstatistic.VectorSummarizerUtil;
import com.alibaba.alink.common.utils.TableUtil;
import com.alibaba.alink.operator.batch.BatchOperator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * Util for batch statistical calculation.
 */
public class StatisticsHelper {
    /**
     * Transform cols or vector col to vector and compute summary.
     * it deal with table which only have number cols and not has missing value.
     * It return tuple2, f0 is data, f1 is summary. data f0 is vector, data f1 is row which is reserved cols.
     */
    public static Tuple2<DataSet<Tuple2<Vector, Row>>, DataSet<BaseVectorSummary>> summaryHelper(BatchOperator in,
                                                                                                 String[] selectedColNames,
                                                                                                 String vectorColName,
                                                                                                 String[] reservedColNames) {
        DataSet<Tuple2<Vector, Row>> data = transformToVector(in, selectedColNames, vectorColName, reservedColNames);

        DataSet<Vector> vectorDataSet = data
            .map(new MapFunction<Tuple2<Vector, Row>, Vector>() {
                @Override
                public Vector map(Tuple2<Vector, Row> tuple2) {
                    return tuple2.f0;
                }
            });

        return Tuple2.of(data, summary(vectorDataSet));
    }

    /**
     * Transform to vector without reservedColNames and compute summary.
     * it deal with table which only have number cols and has no missing value.
     */
    public static Tuple2<DataSet<Vector>, DataSet<BaseVectorSummary>> summaryHelper(BatchOperator in,
                                                                                    String[] selectedColNames,
                                                                                    String vectorColName) {
        DataSet<Vector> data = transformToVector(in, selectedColNames, vectorColName);
        return Tuple2.of(data, summary(data));
    }

    /**
     * table summary, selectedColNames must be set.
     */
    public static DataSet<TableSummary> summary(BatchOperator in, String[] selectedColNames) {
        return summarizer(in, selectedColNames, false)
            .map(new MapFunction<TableSummarizer, TableSummary>() {
                @Override
                public TableSummary map(TableSummarizer summarizer) throws Exception {
                    return summarizer.toSummary();
                }
            }).name("toSummary");
    }

    /**
     * vector stat, selectedColName must be set.
     */
    public static DataSet<BaseVectorSummary> vectorSummary(BatchOperator in, String selectedColName) {
        return vectorSummarizer(in, selectedColName, false)
            .map(new MapFunction<BaseVectorSummarizer, BaseVectorSummary>() {
                @Override
                public BaseVectorSummary map(BaseVectorSummarizer summarizer) {
                    return summarizer.toSummary();
                }
            }).name("toSummary");
    }

    /**
     * given vector dataSet, return vector summary.
     */
    public static DataSet<BaseVectorSummary> summary(DataSet<Vector> data) {
        return summarizer(data, false)
            .map(new MapFunction<BaseVectorSummarizer, BaseVectorSummary>() {
                @Override
                public BaseVectorSummary map(BaseVectorSummarizer summarizer) throws Exception {
                    return summarizer.toSummary();
                }
            }).name("toSummary");
    }

    /**
     * calculate correlation. result is tuple2, f0 is summary, f1 is correlation.
     */
    public static DataSet<Tuple2<TableSummary, CorrelationResult>> pearsonCorrelation(BatchOperator in, String[] selectedColNames) {
        return summarizer(in, selectedColNames, true)
            .map(new MapFunction<TableSummarizer, Tuple2<TableSummary, CorrelationResult>>() {
                @Override
                public Tuple2<TableSummary, CorrelationResult> map(TableSummarizer summarizer) {
                    return Tuple2.of(summarizer.toSummary(), summarizer.correlation());
                }
            });
    }

    /**
     * calculate correlation. result is tuple2, f0 is summary, f1 is correlation.
     */
    public static DataSet<Tuple2<BaseVectorSummary, CorrelationResult>> vectorPearsonCorrelation(BatchOperator in, String selectedColName) {
        return vectorSummarizer(in, selectedColName, true)
            .map(new MapFunction<BaseVectorSummarizer, Tuple2<BaseVectorSummary, CorrelationResult>>() {
                @Override
                public Tuple2<BaseVectorSummary, CorrelationResult> map(BaseVectorSummarizer summarizer) {
                    return Tuple2.of(summarizer.toSummary(), summarizer.correlation());
                }
            });
    }

    /**
     * transform to vector.
     */
    public static DataSet<Vector> transformToVector(BatchOperator<?> in,
                                                    String[] selectedColNames,
                                                    String vectorColName) {

        checkSimpleStatParameter(in, selectedColNames, vectorColName, null);

        if (selectedColNames != null && selectedColNames.length != 0) {
            int[] selectedColIndices = TableUtil.findColIndices(in.getColNames(), selectedColNames);
            return in.getDataSet().map(new ColsToVectorWithoutReservedColsMap(selectedColIndices));
        } else {
            int selectColIndex = TableUtil.findColIndex(in.getColNames(), vectorColName);
            return in.getDataSet().map(new VectorCoToVectorWithoutReservedColsMap(selectColIndex));
        }
    }

    /**
     * transform to vector, result is tuple2, f0 is vector, f1 is reserved cols.
     */
    public static DataSet<Tuple2<Vector, Row>> transformToVector(BatchOperator<?> in,
                                                                 String[] selectedColNames,
                                                                 String vectorColName,
                                                                 String[] reservedColNames) {

        checkSimpleStatParameter(in, selectedColNames, vectorColName, reservedColNames);

        int[] reservedColIndices = TableUtil.findColIndices(in.getColNames(), reservedColNames);

        if (selectedColNames != null && selectedColNames.length != 0) {
            int[] selectedColIndices = TableUtil.findColIndices(in.getColNames(), selectedColNames);
            return in.getDataSet()
                .map(new ColsToVectorWithReservedColsMap(selectedColIndices, reservedColIndices))
                .name("transform_data");
        } else {
            int vectorColIndex = TableUtil.findColIndex(in.getColNames(), vectorColName);
            return in.getDataSet()
                .map(new VectorColToVectorWithReservedColsMap(vectorColIndex, reservedColIndices))
                .name("transform_data");
        }
    }

    /**
     * Transform vector or table columns to table columns.
     * selectedCols and vectorCol only be one non-empty, reserved cols can be empty.
     * <p>
     * If selected cols is not null, it will combines selected cols and reserved cols,
     * and selected cols will transform to double type.
     * <p>
     * If vector col is not null,  it will splits vector to cols and combines with reserved cols.
     * <p>
     * If selected cols and vector col both set, it will use vector col.
     */
    public static DataSet<Row> transformToColumns(BatchOperator in,
                                                  String[] selectedColNames,
                                                  String vectorColName,
                                                  String[] reservedColNames) {
        checkSimpleStatParameter(in, selectedColNames, vectorColName, reservedColNames);

        int[] reservedColIndices = null;
        if (reservedColNames != null) {
            reservedColIndices = TableUtil.findColIndices(in.getColNames(), reservedColNames);
        }

        if (selectedColNames != null && selectedColNames.length != 0) {
            int[] selectedColIndices = TableUtil.findColIndices(in.getSchema(), selectedColNames);
            return in.getDataSet().map(new ColsToDoubleColsMap(selectedColIndices, reservedColIndices));
        }

        if (vectorColName != null) {
            int vectorColIndex = TableUtil.findColIndex(in.getColNames(), vectorColName);
            return in.getDataSet().map(new VectorColToTableMap(vectorColIndex, reservedColIndices));
        }

        throw new InvalidParameterException("selectedColName and vectorColName must be set one only.");
    }

    /**
     * check parameters is invalid.
     */
    private static void checkSimpleStatParameter(BatchOperator in,
                                                 String[] selectedColNames,
                                                 String vectorColName,
                                                 String[] reservedColNames) {
        if (selectedColNames != null && selectedColNames.length != 0 && vectorColName != null) {
            throw new InvalidParameterException("selectedColName and vectorColName must be set one only.");
        }

        TableUtil.assertSelectedColExist(in.getColNames(), selectedColNames);
        TableUtil.assertNumericalCols(in.getSchema(), selectedColNames);

        TableUtil.assertSelectedColExist(in.getColNames(), vectorColName);
        TableUtil.assertVectorCols(in.getSchema(), vectorColName);

        TableUtil.assertSelectedColExist(in.getColNames(), reservedColNames);
    }

    /**
     * table stat
     */
    private static DataSet<TableSummarizer> summarizer(BatchOperator in, String[] selectedColNames, boolean calculateOuterProduct) {
        if (selectedColNames == null || selectedColNames.length == 0) {
            throw new InvalidParameterException("selectedColNames must be set.");
        }

        in = in.select(selectedColNames);

        return summarizer(in.getDataSet(), calculateOuterProduct, getNumericalColIndices(in.getColTypes()), selectedColNames);
    }

    /**
     * vector stat
     */
    private static DataSet<BaseVectorSummarizer> vectorSummarizer(BatchOperator in, String selectedColName,
                                                                  boolean calculateOuterProduct) {
        if (TableUtil.findColIndex(in.getColNames(), selectedColName) < 0) {
            throw new RuntimeException(selectedColName + " is not exist.");
        }

        DataSet<Vector> data = transformToVector(in, null, selectedColName);
        return summarizer(data, calculateOuterProduct);
    }

    /**
     * given vector dataSet, return vector summary. if outerProduct is true, calculate covariance.
     */
    private static DataSet<BaseVectorSummarizer> summarizer(DataSet<Vector> data, boolean bCov) {
        return data
            .mapPartition(new VectorSummarizerPartition(bCov)).name("summarizer_map")
            .reduce(new ReduceFunction<BaseVectorSummarizer>() {
                @Override
                public BaseVectorSummarizer reduce(BaseVectorSummarizer value1, BaseVectorSummarizer value2)
                    throws Exception {
                    return VectorSummarizerUtil.merge(value1, value2);
                }
            })
            .name("summarizer_reduce");
    }


    /**
     * given data, return summary. numberIndices is the indices of cols which are number type in selected cols.
     */
    private static DataSet<TableSummarizer> summarizer(DataSet<Row> data, boolean bCov, int[] numberIndices,
                                                       String[] selectedColNames) {
        return data
            .mapPartition(new TableSummarizerPartition(bCov, numberIndices, selectedColNames))
            .reduce(new ReduceFunction<TableSummarizer>() {
                @Override
                public TableSummarizer reduce(TableSummarizer left, TableSummarizer right) {
                    return TableSummarizer.merge(left, right);
                }
            });
    }

    /**
     * get indices which type is number
     */
    private static int[] getNumericalColIndices(TypeInformation[] colTypes) {
        List<Integer> numberColIndicesList = new ArrayList<>();
        for (int i = 0; i < colTypes.length; i++) {
            if (TableUtil.isNumber(colTypes[i])) {
                numberColIndicesList.add(i);
            }
        }

        int[] numberColIndices = new int[numberColIndicesList.size()];
        for (int i = 0; i < numberColIndices.length; i++) {
            numberColIndices[i] = numberColIndicesList.get(i);
        }
        return numberColIndices;
    }

    /**
     * MapFunction for Transform cols to vector. result it tuple, f0 is vector, f1 is reserved cols.
     */
    private static class ColsToVectorWithReservedColsMap implements MapFunction<Row, Tuple2<Vector, Row>> {
        private int[] selectedColIndices;
        private int[] reservedColIndices;

        ColsToVectorWithReservedColsMap(int[] selectedColIndices, int[] reservedColIndices) {
            this.selectedColIndices = selectedColIndices;
            this.reservedColIndices = reservedColIndices;
        }

        @Override
        public Tuple2<Vector, Row> map(Row in) throws Exception {
            DenseVector dv = new DenseVector(selectedColIndices.length);
            for (int i = 0; i < selectedColIndices.length; i++) {
                dv.set(i, ((Number) in.getField(this.selectedColIndices[i])).doubleValue());
            }

            Row out = new Row(reservedColIndices.length);
            for (int i = 0; i < this.reservedColIndices.length; ++i) {
                out.setField(i, in.getField(this.reservedColIndices[i]));
            }
            return new Tuple2<>(dv, out);
        }
    }

    /**
     * MapFunction for Transform cols to vector.
     */
    private static class ColsToVectorWithoutReservedColsMap implements MapFunction<Row, Vector> {
        private int[] selectedColIndices;

        ColsToVectorWithoutReservedColsMap(int[] selectedColIndices) {
            this.selectedColIndices = selectedColIndices;
        }

        @Override
        public Vector map(Row in) throws Exception {
            DenseVector dv = new DenseVector(selectedColIndices.length);
            double[] data = dv.getData();
            for (int i = 0; i < selectedColIndices.length; i++) {
                Object obj = in.getField(this.selectedColIndices[i]);
                if (obj instanceof Number) {
                    data[i] = ((Number) obj).doubleValue();
                }
            }

            return dv;
        }
    }

    /**
     * MapFunction for vector col transform to vector. result is tuple2, f0 is vector, f1 is reserved cols.
     */
    private static class VectorColToVectorWithReservedColsMap implements MapFunction<Row, Tuple2<Vector, Row>> {
        private int vectorColIndex;
        private int[] reservedColIndices;

        VectorColToVectorWithReservedColsMap(int vectorColIndex, int[] reservedColIndices) {
            this.vectorColIndex = vectorColIndex;
            this.reservedColIndices = reservedColIndices;
        }

        @Override
        public Tuple2<Vector, Row> map(Row in) throws Exception {
            Vector vec = VectorUtil.getVector(in.getField(vectorColIndex));
            if (vec == null) {
                throw new RuntimeException(
                    "vector is null, please check your input data.");
            }
            Row out = new Row(reservedColIndices.length);
            for (int i = 0; i < this.reservedColIndices.length; ++i) {
                out.setField(i, in.getField(this.reservedColIndices[i]));
            }
            return Tuple2.of(vec, out);
        }
    }

    /**
     * MapFunction for vector col transform to vector.
     */
    private static class VectorCoToVectorWithoutReservedColsMap implements MapFunction<Row, Vector> {
        private int vectorColIndex;

        VectorCoToVectorWithoutReservedColsMap(int vectorColIndex) {
            this.vectorColIndex = vectorColIndex;
        }

        @Override
        public Vector map(Row in) throws Exception {
            return VectorUtil.getVector(in.getField(vectorColIndex));
        }
    }

    /**
     * Transform vector col to table columns.
     */
    private static class VectorColToTableMap implements MapFunction<Row, Row> {

        /**
         * vector col index.
         */
        private int vectorColIndex;
        /**
         * reserved col indices.
         */
        private int[] reservedColIndices;

        VectorColToTableMap(int vectorColIndex, int[] reservedColIndices) {
            this.vectorColIndex = vectorColIndex;
            this.reservedColIndices = reservedColIndices;
            if (reservedColIndices == null) {
                this.reservedColIndices = new int[0];
            }
        }

        @Override
        public Row map(Row in) throws Exception {
            Row out;

            Vector vector = VectorUtil.getVector(in.getField(vectorColIndex));

            DenseVector feature;

            if (vector instanceof DenseVector) {
                feature = (DenseVector) vector;
            } else {
                feature = ((SparseVector) vector).toDenseVector();
            }

            if (feature.getData() != null) {
                out = new Row(feature.size() + reservedColIndices.length);
                for (int i = 0; i < feature.size(); i++) {
                    out.setField(i, feature.get(i));
                }
                for (int i = 0; i < reservedColIndices.length; i++) {
                    out.setField(i + feature.size(), in.getField(reservedColIndices[i]));
                }
            } else {
                out = new Row(reservedColIndices.length);
                for (int i = 0; i < reservedColIndices.length; i++) {
                    out.setField(i, in.getField(reservedColIndices[i]));
                }
            }

            return out;
        }
    }

    /**
     * MapFunction for cols.
     */
    private static class ColsToDoubleColsMap implements MapFunction<Row, Row> {
        private int[] selectedColIndices;
        private int[] reservedColIndices;

        ColsToDoubleColsMap(int[] selectedColIndices, int[] reservedColIndices) {
            this.selectedColIndices = selectedColIndices;
            this.reservedColIndices = reservedColIndices;
            if (reservedColIndices == null) {
                this.reservedColIndices = new int[0];
            }
        }

        @Override
        public Row map(Row in) throws Exception {
            //table cols and reserved cols, table cols will be transform to double type.
            Row out = new Row(selectedColIndices.length + reservedColIndices.length);
            for (int i = 0; i < this.selectedColIndices.length; ++i) {
                out.setField(i, ((Number) in.getField(this.selectedColIndices[i])).doubleValue());
            }
            for (int i = 0; i < reservedColIndices.length; i++) {
                out.setField(i + selectedColIndices.length, in.getField(reservedColIndices[i]));
            }
            return out;
        }
    }

    /**
     * It is table summary partition of one worker, will merge result later.
     */
    public static class TableSummarizerPartition implements MapPartitionFunction<Row, TableSummarizer> {
        private boolean outerProduct;
        private int[] numericalIndices;
        private String[] selectedColNames;

        TableSummarizerPartition(boolean outerProduct, int[] numericalIndices, String[] selectedColNames) {
            this.outerProduct = outerProduct;
            this.numericalIndices = numericalIndices;
            this.selectedColNames = selectedColNames;
        }

        @Override
        public void mapPartition(Iterable<Row> iterable, Collector<TableSummarizer> collector) throws Exception {
            TableSummarizer srt = new TableSummarizer(selectedColNames, numericalIndices, outerProduct);
            srt.colNames = selectedColNames;
            for (Row sv : iterable) {
                srt = (TableSummarizer) srt.visit(sv);
            }

            collector.collect(srt);
        }
    }

    /**
     * It is vector summary partition of one worker, will merge result later.
     */
    public static class VectorSummarizerPartition implements MapPartitionFunction<Vector, BaseVectorSummarizer> {
        private boolean outerProduct;

        public VectorSummarizerPartition(boolean outerProduct) {
            this.outerProduct = outerProduct;
        }

        @Override
        public void mapPartition(Iterable<Vector> iterable, Collector<BaseVectorSummarizer> collector) throws Exception {
            BaseVectorSummarizer srt = new DenseVectorSummarizer(outerProduct);
            for (Vector sv : iterable) {
                srt = srt.visit(sv);
            }

            collector.collect(srt);
        }
    }


}